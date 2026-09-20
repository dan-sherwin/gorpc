package gorpc

import "fmt"

// streamFlow is protected by Stream.mu. Each direction has its own window.
// The first grant establishes the limits; later grants only return credit.
type streamFlow struct {
	initialized bool
	items       uint64
	bytes       uint64
	maxItems    uint64
	maxBytes    uint64
	changed     chan struct{}

	returnItems uint64
	returnBytes uint64
}

func (s *Stream) receiveWindow(frame *Frame) {
	if s.flow != nil && s.receivesItems {
		frame.WindowItems = uint64(cap(s.recvCh))
		frame.WindowBytes = s.recvByteLimit
	}
}

func (s *Stream) grantInitialCredit() error {
	if s.flow == nil || !s.receivesItems {
		return nil
	}
	frame := Frame{Type: FrameStreamWindow, RequestID: s.requestID}
	s.receiveWindow(&frame)
	return s.write(frame)
}

func (s *Stream) returnCredit(size uint64) error {
	s.mu.Lock()
	f := s.flow
	if f == nil || s.recvClosed || s.done {
		s.mu.Unlock()
		return nil
	}
	f.returnItems++
	f.returnBytes += size
	// Return half a window at a time, but never hold credit on an empty queue:
	// the next item might need more bytes than the sender has left.
	if f.returnItems < (uint64(cap(s.recvCh))+1)/2 && f.returnBytes < (s.recvByteLimit+1)/2 && len(s.recvCh) > 0 {
		s.mu.Unlock()
		return nil
	}
	frame := Frame{
		Type: FrameStreamWindow, RequestID: s.requestID,
		WindowItems: f.returnItems, WindowBytes: f.returnBytes,
	}
	f.returnItems, f.returnBytes = 0, 0
	s.mu.Unlock()
	return s.write(frame)
}

func (s *Stream) acceptCredit(frame Frame) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.done || s.sendClosed {
		return nil
	}
	f := s.flow
	if f == nil || !s.sendsItems {
		return fmt.Errorf("%w: unexpected stream credit", ErrProtocol)
	}
	if !f.initialized {
		if frame.WindowItems == 0 || frame.WindowBytes == 0 {
			return fmt.Errorf("%w: empty stream window", ErrProtocol)
		}
		f.initialized = true
		f.maxItems, f.maxBytes = frame.WindowItems, frame.WindowBytes
	} else if frame.WindowItems == 0 || frame.WindowItems > f.maxItems-f.items || frame.WindowBytes > f.maxBytes-f.bytes {
		return fmt.Errorf("%w: invalid stream credit", ErrProtocol)
	}
	f.items += frame.WindowItems
	f.bytes += frame.WindowBytes
	// Send is serialized by sendMu, so at most one sender needs waking.
	select {
	case f.changed <- struct{}{}:
	default:
	}
	return nil
}

func (s *Stream) takeCredit(size uint64) error {
	for {
		if err := s.sendError(); err != nil {
			return err
		}
		s.mu.Lock()
		f := s.flow
		if f == nil {
			s.mu.Unlock()
			return nil
		}
		if f.initialized && size > f.maxBytes {
			s.mu.Unlock()
			return fmt.Errorf("%w: stream item exceeds the receiver's %d-byte window", ErrFrameTooLarge, f.maxBytes)
		}
		if f.items > 0 && size <= f.bytes {
			f.items--
			f.bytes -= size
			s.mu.Unlock()
			return nil
		}
		changed := f.changed
		s.mu.Unlock()
		select {
		case <-changed:
		case <-s.sendDone:
		case <-s.ctx.Done():
		}
	}
}
