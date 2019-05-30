package utils

import (
	"errors"
	"strings"
)

const (
	Starting      int = 0x0000
	ScanningItems     = 0x0001
	QuotedString      = 0x0002
	Escaping          = 0x0004
)

func parserParseSingleCharacter(chr byte, builder *strings.Builder, state int, array *[]string) (int, error) {
	if state&Escaping == Escaping {
		// If escaping, just grab the character anc continue
		builder.WriteByte(chr)
		// Unset escaping if it's set otherwise
		// do nothing
		state &= ^Escaping
		// Just go to the next character now
		return state, nil
	} else if state&QuotedString == QuotedString && chr != '"' {
		// If inside a quoted string, just read the value
		builder.WriteByte(chr)
		// Continue with the next character too
		return state, nil
	}
	// None of the above happened, so we need to check what to do
	switch chr {
	case '{':
		if state != Starting {
			return state, errors.New("unexpected `{' at in the middle of the array")
		}
		state = ScanningItems
		break
	case '}':
		if state == ScanningItems {
			*array = append(*array, builder.String())
			// We're done here
			return state, nil
		} else {
			return state, errors.New("unexpected `}'")
		}
	case ',':
		*array = append(*array, builder.String())
		// Now we must reset this
		builder.Reset()
		break
	case '"':
		// Swap the quoted string state bit
		if state&QuotedString == QuotedString {
			state &= ^QuotedString
		} else {
			state |= QuotedString
		}
		break
	case ' ':
	case '\t':
	case '\n':
	case '\r':
		break
	default:
		builder.WriteByte(chr)
	}
	return state, nil
}

func ParseArray(data []byte, array *[]string) error {
	var err error
	var builder strings.Builder
	var state int
	state = Starting
	for _, chr := range data {
		state, err = parserParseSingleCharacter(chr, &builder, state, array)
		if err != nil {
			return err
		}
	}
	return nil
}
