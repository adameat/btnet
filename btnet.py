#!/usr/bin/python
import bluetooth
import socket
import sys
import threading
import time
import signal
import json
import base64

sockets = {}

def crc16(data):
    '''
    CRC-16-ModBus Algorithm
    '''
    data = bytearray(data.encode())
    poly = 0xA001
    crc = 0xFFFF
    for b in data:
        crc ^= (0xFF & b)
        for _ in range(0, 8):
            if (crc & 0x0001):
                crc = ((crc >> 1) & 0xFFFF) ^ poly
            else:
                crc = ((crc >> 1) & 0xFFFF)

    return crc & 0xFFFF


def signalHandler(sig, frame):
    print('Interrupted')
    sys.exit(0)


def controlConnectionLoop(connectionSocket, address):
    try:
        connectionSocket.settimeout(30)
        buffer = '';
        print('[CONTROL] Control connection opened')
        while True:
            data = connectionSocket.recv(1)
            if len(data) == 0:
                break
            buffer += data.decode()
            if buffer.endswith('\n'):
                buffer = buffer.rstrip()
                if len(buffer) == 0:
                    continue
                parts = buffer.split(' ')
                if len(parts) == 0:
                    continue
                if parts[0] == 'LIST':
                    for name in sockets.keys():
                        connectionSocket.sendall(name + '\n')
                    connectionSocket.sendall('DONE\n')
                if parts[0] == 'SEND':
                    device = sockets.get(parts[1])
                    if device != None:
                        command = ' '.join(parts[2:])
                        print('[%s] < %s' % (parts[1], command))
                        device.sendall(command + '\n')

                buffer = ''

    except Exception as e:
        print('[CONTROL] %s' % e)

    print('[CONTROL] Control connection closed')

    connectionSocket.close()
    return


def controlListenLoop(controlPort):
    listenSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listenSocket.bind(('', controlPort))
    listenSocket.listen(5)
    while True:
        connectionSocket, address = listenSocket.accept()
        thread = threading.Thread(target=controlConnectionLoop, args=(connectionSocket, address))
        thread.daemon = True
        thread.start()

    listenSocket.close()
    return


def deviceLoop(args):
    carbon = None
    device = None
    name = args['name']
    address = args['address']
    mode = args['mode']
    period = args.get('period', 30)
    timeout = args.get('timeout', 60)
    sleep = args.get('sleep', None)
    warmUp = args.get('warmUp', 5)
    errorWait = args.get('errorWait', 0)
    resetTime = args.get('resetTime', 604800)
    connected = False
    resetErrors = 0
    lastOkTime = 0
    epoch = time.time()
    while True:
        start = time.time()
        try:
            buffer = ''
            connected = False
            wait = 0
            if carbon is None:
                print('[%s] Connecting to carbon %s' % (name, args['carbon']))
                addr = args['carbon'].split(':')
                new_carbon = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                new_carbon.connect((addr[0], int(addr[1])))
                carbon = new_carbon
                print('[%s] Connected to carbon' % (name))
            print('[%s] Connecting to device %s' % (name, address))
            new_device = bluetooth.BluetoothSocket(bluetooth.RFCOMM)
            new_device.connect((address, 1))
            device = new_device
            device.settimeout(timeout)
            print('[%s] Connected to device %s (timeout %.0f)' % (name, address, timeout))
            sockets[name] = device;
            connected = True
            if mode == 'READ':
                if sleep:
                    tries = 0
                    while tries < 3:
                        ++tries
                        print('[%s] < PING' % name)
                        device.send("PING\n")
                        buffer = ''
                        while True:
                            data = device.recv(1)
                            if len(data) == 0:
                                break
                            if (data[0] < 32 or data[0] > 127) and data[0] != 10:
                                print('[%s] > %s' % (name, buffer))
                                break
                            buffer += data.decode()
                            if buffer.endswith('\n'):
                                buffer = buffer.rstrip()
                                print('[%s] > %s' % (name, buffer))
                                break
                        if buffer == 'PONG':
                            break
                    if buffer != 'PONG':
                        raise Exception('Could not communicate to device')

                print('[%s] < %s' % (name, mode))
                device.send("%s\n" % mode)
            elif mode == 'FEED':
                print('[%s] < %s %d' % (name, mode, period))
                device.send("%s %d\n" % (mode, period))
            elif mode == 'RESET':
                connected = False
                mode = args['mode']
                wait = errorWait
                print('[%s] < RESET' % name)
                device.send("RESET\n")
            lastOkTime = int(time.time())
            buffer = ''
            while connected:
                data = device.recv(1)
                if len(data) == 0:
                    break
                if (data[0] < 32 or data[0] > 127) and data[0] != 10:
                    print('[%s] > %s' % (name, buffer))
                    buffer = ''
                    continue
                buffer += data.decode()
                if buffer.endswith('\n'):
                    buffer = buffer.rstrip()
                    if len(buffer) == 0:
                        continue
                    print('[%s] > %s' % (name, buffer))
                    if buffer == 'PING':
                        continue
                    if buffer == 'PONG':
                        continue
                    if buffer == 'AT':
                        mode = args['mode']
                        print('[%s] Invalid command' % name)
                        break
                    parts = buffer.split(' ')
                    if len(parts) == 0 or parts[0] == 'DONE':
                        if mode == 'READ':
                            if sleep:
                                spent = time.time() - start
                                if spent < period:
                                    wait = period - spent
                                else:
                                    wait = period
                                amount = int(wait - warmUp)
                                if amount > 0:
                                    print('[%s] < SLEEP %d' % (name, amount))
                                    device.send("SLEEP %d\n" % (amount))
                            break
                    if parts[0] == 'DATA' and parts[3] == 'OK':
                        last_part = parts[len(parts) - 1]
                        data_part = buffer[0:len(buffer) - len(last_part) - 1]
                        if len(last_part) <= 2:
                            if len(data_part) != int(last_part):
                                print('[%s] Error in stream, len %d != %d' % (name, len(data_part), int(last_part)))
                                break
                        if len(last_part) == 4:
                            if crc16(data_part) != int(last_part, 16):
                                print('[%s] Error in stream, crc16 %d != %d' % (name, crc16(data_part), int(last_part, 16)))
                                break
                        carbon_data = '{0:s}.{1:s} {2:.2f} {3:d}'.format(name, parts[1], float(parts[2]), int(time.time()))
                        carbon.send(('%s\n' % carbon_data).encode())
                        carbon.send(('{0:s}.good 1.0 {1:d}\n'.format(name, int(time.time()))).encode())
                        resetErrors = 0
                        if lastOkTime < int(time.time()) - 60:
                            print('[%s] < OK' % name)
                            device.send("OK\n")
                            lastOkTime = int(time.time())

                    buffer = ''
            device.close()
            device = None
            print('[%s] Disconnected' % name)
            del sockets[name]

        except Exception as e:
            print('[%s] Error' % name)
            print('[%s] %s' % (name, e))
            if device is not None:
                device.close()
                device = None
                del sockets[name]
            if carbon is not None:
                try:
                    carbon.send('{0:s}.errors 1.0 {1:d}\n'.format(name, int(time.time())))
                    if connected:
                        carbon.send('{0:s}.resets 1.0 {1:d}\n'.format(name, int(time.time())))
                except Exception as e:
                    pass

                carbon.close()
                carbon = None
            print('[%s] Disconnected' % name)
            if mode == 'RESET':
                mode = args['mode']
            else:
                wait = errorWait
            if connected:
                resetErrors += 1
                print('[%s] Error while connected #%d' % (name, resetErrors))
                if resetErrors >= 3:
                    try:
                        if carbon is not None:
                            carbon.send('{0:s}.resets 1.0 {1:d}\n'.format(name, int(time.time())))
                    except Exception as e:
                        pass
                    mode = 'RESET'
                    print('[%s] Will reset on reconnect because of errors' % name)
                    resetErrors = 0
            pass

        now = time.time()
        if now - epoch > resetTime:
            epoch = now
            mode = 'RESET'
            print('[%s] Will reset on reconnect because of resetTime' % name)

        spent = now - start
        if wait == 0:
            if spent < period:
                wait = period - spent
            else:
                wait = period
        print('[%s] Spent %d seconds, waiting for %.0f seconds (period %.0f)' % (name, spent, wait, period))
        time.sleep(wait)
    return


signal.signal(signal.SIGINT, signalHandler)
config = json.load(open('btnet.json'))
thread = threading.Thread(target=controlListenLoop, args=(1846,))
thread.daemon = True
thread.start()


for device in config['devices']:
    args = {}
    args.update(config['settings'])
    args.update(device)
    thread = threading.Thread(target=deviceLoop, args=(args,))
    thread.daemon = True
    thread.start()
    time.sleep(1)


signal.pause()
