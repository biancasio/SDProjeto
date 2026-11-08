import zmq

# proxy pub/sub: XSUB na 5557 (recebe dos publishers) e XPUB na 5558 (manda pros subscribers)

context = zmq.Context()

xsub = context.socket(zmq.XSUB)
xsub.bind("tcp://*:5557")

xpub = context.socket(zmq.XPUB)
xpub.bind("tcp://*:5558")

print("proxy pub/sub rodando (5557 XSUB / 5558 XPUB)", flush=True)

zmq.proxy(xpub, xsub)
