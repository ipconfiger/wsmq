# wsmq
message queue based on websocket

## Installation

In theory, it can support Mac, Linux, Windows systems, but I don’t have a Windows machine, so I only compiled a linux-musl executable file. If you are using a Linux system, you can download and run it directly. If you are using other systems, you have to git clone the code and compile it yourself.

## Usage

for example:

```
$wget https://  /opt/lib/wsmq
$sudo ln -s /opt/lib/wsmq/wsmq4 /usr/bin/wsmq
$wsmq 8000
```
server will run at port 8000, if you don't pass argument 8000, it will start at default port 8080




