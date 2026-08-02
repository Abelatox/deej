package deej

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jacobsa/go-serial/serial"
	"go.uber.org/zap"

	"github.com/omriharel/deej/pkg/deej/util"
)

// SerialIO provides a deej-aware abstraction layer to managing serial I/O
type SerialIO struct {
	deej   *Deej
	logger *zap.SugaredLogger

	mu          sync.Mutex
	connected   bool
	connOptions serial.OpenOptions
	conn        io.ReadWriteCloser
	cancel      context.CancelFunc

	// tracks whether we've already sent a "can't connect" toast for the current
	// outage, so retries don't spam the user with a notification every few seconds
	disconnectNotified bool

	// tracks whether we've ever lost/failed a connection since deej started, so we
	// only show a "reconnected" toast when it's actually meaningful (not on first connect)
	hadConnectionIssue bool

	lastKnownNumSliders        int
	currentSliderPercentValues []float32

	sliderMoveConsumers []chan SliderMoveEvent
}

// SliderMoveEvent represents a single slider move captured by deej
type SliderMoveEvent struct {
	SliderID     int
	PercentValue float32
}

var expectedLinePattern = regexp.MustCompile(`^\d{1,4}(\|\d{1,4})*\r\n$`)

// how long to wait between (re)connection attempts. this covers both cold-start
// timing issues (deej launching before the OS finishes enumerating the COM port)
// and the arduino getting physically unplugged - either way, deej quietly keeps
// trying instead of requiring a manual reconnect or a program restart
const reconnectRetryDelay = 3 * time.Second

// NewSerialIO creates a SerialIO instance that uses the provided deej
// instance's connection info to establish communications with the arduino chip
func NewSerialIO(deej *Deej, logger *zap.SugaredLogger) (*SerialIO, error) {
	logger = logger.Named("serial")

	sio := &SerialIO{
		deej:                deej,
		logger:              logger,
		connected:           false,
		conn:                nil,
		sliderMoveConsumers: []chan SliderMoveEvent{},
	}

	logger.Debug("Created serial i/o instance")

	// respond to config changes
	sio.setupOnConfigReload()

	return sio, nil
}

// Start attempts to connect to our arduino chip and keeps the connection alive
// in the background: if the initial attempt fails, or the connection later drops
// (arduino unplugged, cable hiccup, OS momentarily reclaiming the port, etc.),
// deej automatically keeps retrying every few seconds on its own - no need to
// physically reconnect anything or restart the program
func (sio *SerialIO) Start() error {
	sio.mu.Lock()
	if sio.cancel != nil {
		sio.mu.Unlock()
		sio.logger.Warn("Already connected, can't start another without closing first")
		return errors.New("serial: connection already active")
	}

	ctx, cancel := context.WithCancel(context.Background())
	sio.cancel = cancel
	sio.mu.Unlock()

	go sio.maintainConnection(ctx)

	return nil
}

// Stop signals us to shut down our serial connection, if one is active, and
// cancels any ongoing reconnection attempts
func (sio *SerialIO) Stop() {
	sio.mu.Lock()
	cancel := sio.cancel
	sio.cancel = nil
	sio.mu.Unlock()

	if cancel != nil {
		sio.logger.Debug("Shutting down serial connection")
		cancel()
	} else {
		sio.logger.Debug("Not currently connected, nothing to stop")
	}
}

// SubscribeToSliderMoveEvents returns an unbuffered channel that receives
// a sliderMoveEvent struct every time a slider moves
func (sio *SerialIO) SubscribeToSliderMoveEvents() chan SliderMoveEvent {
	ch := make(chan SliderMoveEvent)
	sio.sliderMoveConsumers = append(sio.sliderMoveConsumers, ch)

	return ch
}

func (sio *SerialIO) setupOnConfigReload() {
	configReloadedChannel := sio.deej.config.SubscribeToChanges()

	const stopDelay = 50 * time.Millisecond

	go func() {
		for range configReloadedChannel {

			// make any config reload unset our slider number to ensure process volumes are being re-set
			// (the next read line will emit SliderMoveEvent instances for all sliders)
			// this needs to happen after a small delay, because the session map will also re-acquire sessions
			// whenever the config file is reloaded, and we don't want it to receive these move events while the map
			// is still cleared. this is kind of ugly, but shouldn't cause any issues
			go func() {
				<-time.After(stopDelay)
				sio.lastKnownNumSliders = 0
			}()

			// if connection params have changed, attempt to stop and start the connection
			sio.mu.Lock()
			portChanged := sio.deej.config.ConnectionInfo.COMPort != sio.connOptions.PortName ||
				uint(sio.deej.config.ConnectionInfo.BaudRate) != sio.connOptions.BaudRate
			sio.mu.Unlock()

			if portChanged {
				sio.logger.Info("Detected change in connection parameters, attempting to renew connection")
				sio.Stop()

				// let the connection close
				<-time.After(stopDelay)

				if err := sio.Start(); err != nil {
					sio.logger.Warnw("Failed to renew connection after parameter change", "error", err)
				} else {
					sio.logger.Debug("Renewed connection successfully")
				}
			}
		}
	}()
}

// maintainConnection keeps attempting to (re)connect to the arduino for as long
// as ctx isn't cancelled, waiting reconnectRetryDelay between attempts. a single
// call to connect() blocks for the entire lifetime of one connection (until it
// drops or ctx is cancelled), so this loop only spins during outages
func (sio *SerialIO) maintainConnection(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}

		if err := sio.connect(ctx); err != nil {
			sio.notifyConnectionIssue(err)
		}

		if ctx.Err() != nil {
			return
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(reconnectRetryDelay):
		}
	}
}

// connect opens the serial connection and, if successful, blocks while reading
// lines from it until the connection is lost or ctx is cancelled. it only
// returns a non-nil error when the port itself couldn't be opened
func (sio *SerialIO) connect(ctx context.Context) error {

	// set minimum read size according to platform (0 for windows, 1 for linux)
	// this prevents a rare bug on windows where serial reads get congested,
	// resulting in significant lag
	minimumReadSize := 0
	if util.Linux() {
		minimumReadSize = 1
	}

	connOptions := serial.OpenOptions{
		PortName:        sio.deej.config.ConnectionInfo.COMPort,
		BaudRate:        uint(sio.deej.config.ConnectionInfo.BaudRate),
		DataBits:        8,
		StopBits:        1,
		MinimumReadSize: uint(minimumReadSize),
	}

	sio.logger.Debugw("Attempting serial connection",
		"comPort", connOptions.PortName,
		"baudRate", connOptions.BaudRate,
		"minReadSize", minimumReadSize)

	conn, err := serial.Open(connOptions)
	if err != nil {
		return fmt.Errorf("open serial connection: %w", err)
	}

	namedLogger := sio.logger.Named(strings.ToLower(connOptions.PortName))
	namedLogger.Infow("Connected", "conn", conn)

	sio.mu.Lock()
	sio.connOptions = connOptions
	sio.conn = conn
	sio.connected = true
	wasIssue := sio.hadConnectionIssue
	sio.disconnectNotified = false
	sio.mu.Unlock()

	// if we're recovering from a previous outage, let the user know we're back
	if wasIssue {
		sio.deej.notifier.Notify(
			"deej reconnected",
			fmt.Sprintf("Successfully reconnected to %s.", connOptions.PortName))
	}

	connReader := bufio.NewReader(conn)
	lineChannel := sio.readLine(ctx, namedLogger, connReader)

	for {
		select {
		case <-ctx.Done():
			sio.close(namedLogger)
			return nil
		case line, ok := <-lineChannel:
			if !ok {
				// the read loop ended on its own - the connection was lost
				// (arduino unplugged, cable issue, port disappeared, etc.)
				sio.mu.Lock()
				sio.hadConnectionIssue = true
				sio.mu.Unlock()

				sio.close(namedLogger)
				namedLogger.Warn("Serial connection lost, will attempt to reconnect")
				return nil
			}

			sio.handleLine(namedLogger, line)
		}
	}
}

func (sio *SerialIO) close(logger *zap.SugaredLogger) {
	sio.mu.Lock()
	defer sio.mu.Unlock()

	sio.connected = false

	if sio.conn == nil {
		return
	}

	if err := sio.conn.Close(); err != nil {
		logger.Warnw("Failed to close serial connection", "error", err)
	} else {
		logger.Debug("Serial connection closed")
	}

	sio.conn = nil
}

// notifyConnectionIssue sends a single toast notification for the current outage
// (initial connection failure or a dropped connection) without spamming the user
// on every retry attempt
func (sio *SerialIO) notifyConnectionIssue(err error) {
	sio.mu.Lock()
	if sio.disconnectNotified {
		sio.mu.Unlock()
		return
	}
	sio.disconnectNotified = true
	sio.hadConnectionIssue = true
	portName := sio.deej.config.ConnectionInfo.COMPort
	sio.mu.Unlock()

	sio.logger.Warnw("Serial connection unavailable, will keep retrying in the background", "error", err)

	message := fmt.Sprintf("Couldn't connect to %s - deej will keep trying automatically.", portName)

	switch {
	case errors.Is(err, os.ErrPermission):
		message = fmt.Sprintf("%s is busy - close any serial monitor or other deej instance. Retrying automatically.", portName)
	case errors.Is(err, os.ErrNotExist):
		message = fmt.Sprintf("%s not found - check your configuration or reconnect the device. Retrying automatically.", portName)
	}

	sio.deej.notifier.Notify("deej can't connect", message)
}

func (sio *SerialIO) readLine(ctx context.Context, logger *zap.SugaredLogger, reader *bufio.Reader) chan string {
	ch := make(chan string)

	go func() {
		defer close(ch)

		for {
			line, err := reader.ReadString('\n')
			if err != nil {

				if sio.deej.Verbose() {
					logger.Warnw("Failed to read line from serial", "error", err, "line", line)
				}

				// the read loop stops after this - connect() will notice the
				// channel closed and treat it as a dropped connection, triggering
				// an automatic reconnect attempt
				return
			}

			if sio.deej.Verbose() {
				logger.Debugw("Read new line", "line", line)
			}

			// deliver the line to the channel, unless we're shutting down
			select {
			case ch <- line:
			case <-ctx.Done():
				return
			}
		}
	}()

	return ch
}

func (sio *SerialIO) handleLine(logger *zap.SugaredLogger, line string) {

	// this function receives an unsanitized line which is guaranteed to end with LF,
	// but most lines will end with CRLF. it may also have garbage instead of
	// deej-formatted values, so we must check for that! just ignore bad ones
	if !expectedLinePattern.MatchString(line) {
		return
	}

	// trim the suffix
	line = strings.TrimSuffix(line, "\r\n")

	// split on pipe (|), this gives a slice of numerical strings between "0" and "1023"
	splitLine := strings.Split(line, "|")
	numSliders := len(splitLine)

	// update our slider count, if needed - this will send slider move events for all
	if numSliders != sio.lastKnownNumSliders {
		logger.Infow("Detected sliders", "amount", numSliders)
		sio.lastKnownNumSliders = numSliders
		sio.currentSliderPercentValues = make([]float32, numSliders)

		// reset everything to be an impossible value to force the slider move event later
		for idx := range sio.currentSliderPercentValues {
			sio.currentSliderPercentValues[idx] = -1.0
		}
	}

	// for each slider:
	moveEvents := []SliderMoveEvent{}
	for sliderIdx, stringValue := range splitLine {

		// convert string values to integers ("1023" -> 1023)
		number, _ := strconv.Atoi(stringValue)

		// turns out the first line could come out dirty sometimes (i.e. "4558|925|41|643|220")
		// so let's check the first number for correctness just in case
		if sliderIdx == 0 && number > 1023 {
			sio.logger.Debugw("Got malformed line from serial, ignoring", "line", line)
			return
		}

		// map the value from raw to a "dirty" float between 0 and 1 (e.g. 0.15451...)
		dirtyFloat := float32(number) / 1023.0

		// normalize it to an actual volume scalar between 0.0 and 1.0 with 2 points of precision
		normalizedScalar := util.NormalizeScalar(dirtyFloat)

		// if sliders are inverted, take the complement of 1.0
		if sio.deej.config.InvertSliders {
			normalizedScalar = 1 - normalizedScalar
		}

		// check if it changes the desired state (could just be a jumpy raw slider value)
		if util.SignificantlyDifferent(sio.currentSliderPercentValues[sliderIdx], normalizedScalar, sio.deej.config.NoiseReductionLevel) {

			// if it does, update the saved value and create a move event
			sio.currentSliderPercentValues[sliderIdx] = normalizedScalar

			moveEvents = append(moveEvents, SliderMoveEvent{
				SliderID:     sliderIdx,
				PercentValue: normalizedScalar,
			})

			if sio.deej.Verbose() {
				logger.Debugw("Slider moved", "event", moveEvents[len(moveEvents)-1])
			}
		}
	}

	// deliver move events if there are any, towards all potential consumers
	if len(moveEvents) > 0 {
		for _, consumer := range sio.sliderMoveConsumers {
			for _, moveEvent := range moveEvents {
				consumer <- moveEvent
			}
		}
	}
}
