package sarama

import "sync"

type MessageBlock struct {
	Offset int64
	Msg    *Message
}

var messageBlockPool = sync.Pool{}

func acquireMessageBlock() *MessageBlock {
	val := messageBlockPool.Get()
	if val != nil {
		return val.(*MessageBlock)
	}
	return &MessageBlock{}
}

func releaseMessageBlock(m *MessageBlock) {
	if m.Msg != nil {
		releaseMessage(m.Msg)
		m.Msg = nil
	}

	messageBlockPool.Put(m)
}

// Messages convenience helper which returns either all the
// messages that are wrapped in this block
func (msb *MessageBlock) Messages() []*MessageBlock {
	if msb.Msg.Set != nil {
		return msb.Msg.Set.Messages
	}
	return []*MessageBlock{msb}
}

func (msb *MessageBlock) encode(pe packetEncoder) error {
	pe.putInt64(msb.Offset)
	pe.push(&lengthField{})
	err := msb.Msg.encode(pe)
	if err != nil {
		return err
	}
	return pe.pop()
}

func (msb *MessageBlock) decode(pd packetDecoder) (err error) {
	if msb.Offset, err = pd.getInt64(); err != nil {
		return err
	}

	lengthDecoder := acquireLengthField()
	if err = pd.push(lengthDecoder); err != nil {
		return err
	}

	msb.Msg = acquireMessage()
	if err = msb.Msg.decode(pd); err != nil {
		return err
	}

	if err = pd.pop(); err != nil {
		return err
	}

	releaseLengthField(lengthDecoder)

	return nil
}

type MessageSet struct {
	PartialTrailingMessage bool // whether the set on the wire contained an incomplete trailing MessageBlock
	OverflowMessage        bool // whether the set on the wire contained an overflow message
	Messages               []*MessageBlock
}

var messageSetPool = sync.Pool{}

func acquireMessageSet() *MessageSet {
	val := messageSetPool.Get()
	if val != nil {
		return val.(*MessageSet)
	}
	return &MessageSet{}
}

func releaseMessageSet(m *MessageSet) {
	for _, mb := range m.Messages {
		releaseMessageBlock(mb)
	}
	m.Messages = m.Messages[:0]

	messageSetPool.Put(m)
}

func (ms *MessageSet) encode(pe packetEncoder) error {
	for i := range ms.Messages {
		err := ms.Messages[i].encode(pe)
		if err != nil {
			return err
		}
	}
	return nil
}

func (ms *MessageSet) decode(pd packetDecoder) (err error) {
	if ms.Messages != nil {
		ms.Messages = ms.Messages[:0]
	}

	for pd.remaining() > 0 {
		magic, err := magicValue(pd)
		if err != nil {
			if err == ErrInsufficientData {
				ms.PartialTrailingMessage = true
				return nil
			}
			return err
		}

		if magic > 1 {
			return nil
		}

		msb := acquireMessageBlock()
		err = msb.decode(pd)
		switch err {
		case nil:
			ms.Messages = append(ms.Messages, msb)
		case ErrInsufficientData:
			// As an optimization the server is allowed to return a partial message at the
			// end of the message set. Clients should handle this case. So we just ignore such things.
			if msb.Offset == -1 {
				// This is an overflow message caused by chunked down conversion
				ms.OverflowMessage = true
			} else {
				ms.PartialTrailingMessage = true
			}
			return nil
		default:
			return err
		}
	}

	return nil
}

func (ms *MessageSet) addMessage(msg *Message) {
	block := acquireMessageBlock()
	block.Msg = msg
	ms.Messages = append(ms.Messages, block)
}
