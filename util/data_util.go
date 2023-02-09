package util

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

func GenerateSixByteTime(t *time.Time) (bs []byte, err error) {
	var timeString string
	if t != nil {
		timeString = t.Format("20060102150405")[2:]
	} else {
		timeString = time.Now().Format("20060102150405")[2:]
	}

	var res string
	for len(timeString) > 0 {
		b := timeString[0:2]
		if strings.HasPrefix(b, "0") {
			res = res + b
		} else {
			value, _ := strconv.ParseInt(b, 10, 64)
			valueString := fmt.Sprintf("%x", value)
			if len(valueString) == 1 {
				valueString = fmt.Sprintf("0%s", valueString)
			}
			res = res + valueString
		}
		timeString = timeString[2:]
	}
	bs, err = hex.DecodeString(res)
	return
}

// 生成下一个流水号
func GenerateNextSerialNumber(serialNumber []byte) (nextSerialNumber []byte, err error) {
	//log.Info().Msgf("serialNumber is: %x", serialNumber)
	data := int64(binary.BigEndian.Uint16(serialNumber))
	if data >= 65535 {
		err = errors.New("byte range error")
		return
	}
	data = data + 1

	//log.Info().Msgf("nextSerialNumber is: %d, %x", data, data)
	nextSerialNumber, err =  hex.DecodeString(fmt.Sprintf("%04x", data))
	return
}

func ConvertInt64ToBytes(data int64) []byte {
	bytesBuffer := bytes.NewBuffer([]byte{})
	binary.Write(bytesBuffer, binary.BigEndian, data)
	return bytesBuffer.Bytes()
}
