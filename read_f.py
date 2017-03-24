from struct import pack, unpack
from pymodbus.client.sync import ModbusSerialClient as ModbusClient
client = ModbusClient(method="rtu", port="/dev/ttyUSB0",baudrate=19200, 
			parity='E',stopbits=1,timeout=1)
client.connect()


ids = range(2,9)[::-1]
data_rq = [
	[3000, 6, 'current'],
	[3020, 18,'voltage'],
	[3054, 6, 'power A,B,C'],
	[3060, 2, 'Pactive'],
	[3068, 2, 'Preactive'],
	[3076, 2, 'Pwhole'],
	[3084, 2, 'CosPhi'],
	[3110, 2, 'Frequency'],
]
for rq in data_rq:
	for id in ids:
		try:
			rr = client.read_holding_registers(rq[0]-1, rq[1], unit=id)
			d = rr.registers
			#print "id=",id,[hex(i) for i in rr.registers]
			print 'id={} {}'.format(id, rq[2]),[round(f, 2) for f in unpack('>{}f'.format(len(d)/2), pack('>{}H'.format(len(d)),*d))]
		except AttributeError:
			print 'id={} error while reading'.format(id)

