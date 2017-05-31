package main

import (
    "bytes"
    "encoding/binary"
    "net"
    "strings"
    "strconv"
    "time"
    "gopkg.in/mgo.v2"
    "fmt"
)

var roomid string

type Packet struct {
    length      int32
    hLength     int32
    hType       int16
    hEncrypt    byte
    hReserved   byte
    data        string
}

func int32ToBytes(num int32) []byte {
    bytesBuffer := bytes.NewBuffer([]byte{})
    binary.Write(bytesBuffer, binary.LittleEndian, num)
    return bytesBuffer.Bytes()
}

func int16ToBytes(num int16) []byte {
    bytesBuffer := bytes.NewBuffer([]byte{})
    binary.Write(bytesBuffer, binary.LittleEndian, num)
    return bytesBuffer.Bytes()
}

func bytesToInt32(bs []byte) int32 {
    bytesBuffer := bytes.NewBuffer(bs)
    var num int32
    binary.Read(bytesBuffer, binary.LittleEndian, &num)
    return num
}

func serialize(p Packet) []byte {
    bytesBuffer := bytes.NewBuffer([]byte{})
    contentBuffer := bytes.NewBuffer([]byte{})
    contentBuffer.WriteString(p.data)
    contentBuffer.WriteByte(0x0)
    content := contentBuffer.Bytes()
    contentLen := len(content)

    bytesBuffer.Write(int32ToBytes((int32)(8 + contentLen)))
    bytesBuffer.Write(int32ToBytes((int32)(8 + contentLen)))
    bytesBuffer.Write(int16ToBytes(p.hType))
    bytesBuffer.WriteByte(p.hEncrypt)
    bytesBuffer.WriteByte(p.hReserved)
    bytesBuffer.Write(content)
    return bytesBuffer.Bytes()
}

func handleResponse(response string) map[string]string {
    pairs := strings.Split(response, "/")
    size := len(pairs)
    params := make(map[string]string)
    for i := 0; i < size; i++ {
        kv := strings.Split(pairs[i], "@=")
        if (len(kv) == 2) {
            key := strings.Replace(strings.Replace(kv[0], "@S", "/", -1), "@A", "@", -1)
            value := strings.Replace(strings.Replace(kv[1], "@S", "/", -1), "@A", "@", -1)
            params[key] = value
        }
    }

    return params
}

func login(conn *net.TCPConn) {
    loginContent := "type@=loginreq/roomid@=" + roomid + "/"
    p := Packet{0, 0, 689, 0x0, 0x0, loginContent}

    sendData := serialize(p)
    _, err := conn.Write(sendData)
    if (err != nil) {
        fmt.Printf("send login message failed: %v\n", err)
    }

    recvBuffer := make([]byte, 1024)
    recvLen, err := conn.Read(recvBuffer)

    content := string(recvBuffer[12:recvLen])
    params := handleResponse(content)
    v, ok := params["type"]
    if (ok && v == "loginres") {
        fmt.Printf("login success\n")
    }
}

func joinGroup(conn *net.TCPConn) {
    joinGroupContent := "type@=joingroup/rid@=" + roomid + "/gid@=-9999/"
    p := Packet{0, 0, 689, 0x0, 0x0, joinGroupContent}

    sendData := serialize(p)
    _, err := conn.Write(sendData)
    if (err != nil) {
        fmt.Printf("send joinGroup message failed: %v\n", err)
    }
}

func sendHeart(conn *net.TCPConn) {
    sendHeartContent := "type@=keeplive/tick@=" + strconv.FormatInt(time.Now().Unix(), 10) + "/"
    p := Packet{0, 0, 689, 0x0, 0x0, sendHeartContent}
    sendData := serialize(p)

    for {
        time.Sleep(45 * time.Second)
        _, err := conn.Write(sendData)
        if (err != nil) {
            fmt.Printf("send heartbeat message failed: %v\n", err)
        }
    }
}

func handleRecvContent(content string, session *mgo.Session) {
    params := handleResponse(content)

    v, ok := params["type"]

    if (ok && v != "error" && v != "keeplive") {
        // fmt.Printf("receive danmu from %v: %v\n", params["nn"], params["txt"])
        collectionName := v + "_" + roomid + "_" + time.Now().Format("20060102")
        err := session.DB("douyu").C(collectionName).Insert(params)
        if (err != nil) {
            fmt.Printf("insert failed: %v\n", err)
        }
    }
}


func main() {

    roomid = "288016"

    session, err := mgo.Dial("mongodb://wanziyooo:wanziyooo@116.62.58.198:27017/douyu")
    if (err != nil) {
        fmt.Printf("connect to mongodb failed: %v\n", err)
    }
    defer session.Close()

    destAddr, err := net.ResolveTCPAddr("tcp", "openbarrage.douyutv.com:8601")
    if (err != nil) {
        fmt.Printf("resolve dest address failed: %v\n", err)
    }

    conn, err := net.DialTCP("tcp", nil, destAddr)
    if (err != nil) {
        fmt.Printf("connect to dest address failed: %v\n", err)
    }
    defer conn.Close()

    login(conn)
    joinGroup(conn)

    go sendHeart(conn)
    fmt.Printf("here")

    bytesBuffer := bytes.NewBuffer([]byte{})
    recvBuffer := make([]byte, 4096)
    for {
        recvLen, err := conn.Read(recvBuffer)
        if (err != nil) {
            fmt.Printf("receive bytes failed: %v\n", err)
        }
        bytesBuffer.Write(recvBuffer[0:recvLen])
        dealBytes := bytesBuffer.Bytes()

        for {
            // Dealing with sticky packet problem 
            if (len(dealBytes) < 4) {
                bytesBuffer.Reset()
                bytesBuffer.Write(dealBytes)
                break
            }
            dataLen := bytesToInt32(dealBytes[0:4]) - 8
            fullSize := 12 + dataLen
            if (fullSize > (int32)(len(dealBytes))) {
                bytesBuffer.Reset()
                bytesBuffer.Write(dealBytes)
                break
            }
            oneMessageBytes := dealBytes[0:fullSize]
            response := string(oneMessageBytes[12:])
            dealBytes = dealBytes[fullSize:]

            // deal message in other goroutine
            go handleRecvContent(response, session)
        }
    }
}