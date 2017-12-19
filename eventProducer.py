#!/usr/bin/python

"""
Program generates random fields described below and prints the resunt in CSV format
"""
import string
from random import *
import datetime
from datetime import timedelta
import csv
import socket
from time import sleep

def produceEvent(priceMu, priceSigma, startDate, timeMu, timeSigma, listIPs):
    """
    Returns the set of output parameters in CSV format: product name, product price,
    purchase date, product category, client IP address. The length of product name and
    prodict category is between lenMin and lenMax. Product price is a result of the
    gaussian distribution with priceMu and priceSigma. Purchase date consists of day:month:year
    and time from 0 to 3600*24 seconds  The range of date is from startDate to startDate + one week.
    The time is the result of gaussian distribution with timeMu and timeSigma.
    The client IP address is choosen randomly from input ip list
    """
    # Product name
    productNumber = randint(1, 100)
    productName = "Product " + str(productNumber)
    #Product price
    productPrice = gauss(priceMu, priceSigma)
    #Purchase time
    randomDate = startDate + timedelta(days=randint(0,6))
    seconds = gauss(timeMu, timeSigma)
    hours = int(seconds//3600)
    seconds = seconds - hours*3600
    minutes = int(seconds//60)
    seconds = int(seconds - minutes*60)
    if hours > 23:
	hours = 23
    #Product category
    productCategory = "Category " + str(int(productNumber / 10))
    #Client IP address
    clientIPaddress = listIPs[randrange(len(listIPs))]
    return ",".join([productName, str(round(productPrice, 2)), randomDate.strftime("%Y-%m-%d") + \
    " {0:02}:{1:02}:{2:02}".format(hours, minutes, seconds), productCategory, clientIPaddress])
    
def readGeoliteTable(filename):
    """
    Reads ip addresses from the file with given filename in CSV format into the list.
    """
    outpList = []
    with open(filename, 'rb') as csvfile:
	ipreader = csv.reader(csvfile)
	next(ipreader, None) # skip the first line
	for row in ipreader:
	    ip = row[0]
	    ip = ip[:ip.index('/')]
	    outpList.append(ip)
    return outpList
    
if __name__ == '__main__':
    ipList = readGeoliteTable("./GeoLite2-Country-CSV_20171107/GeoLite2-Country-Blocks-IPv4.csv")
    host = "127.0.0.1"
    port = 56565
    msgCount = 1000
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((host, port))
    f = open("inputMessages", 'w')
    for x in range(msgCount):
	outpString = produceEvent(25.6, 7.4, datetime.date(2017, 11,5), 17*3600, 4*3600, ipList) + '\n'
	f.write(outpString)
	ack = "FAIL"
	while not ack.startswith("OK"):
	    print "Send:"
	    s.sendall(outpString)
	    ack = s.recv(4096)
	    print "Responce: {0}".format(ack)
	    if not ack.startswith("OK"):
		sleep(30.0)
		
    s.shutdown(socket.SHUT_WR)
    s.close()
    f.close()