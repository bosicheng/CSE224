from scapy.all import *
ip_packet = IP(dst="hackerschool.com", ttl=10)
udp_packet = UDP(dport=40000)
full_packet = IP(dst="hackerschool.com", ttl=10) / UDP(dport=40000)
hostnames = ["www.ucsd.edu","www.cs.ucsd.edu","www.utexas.edu","www.vit.ac.in","www.ntu.edu.sg","www.ethz.ch","www.google.com","www.bing.com","www.facebook.com"]
for index,hostname in enumerate(hostnames):
    print(hostname)
    for i in range(1, 28):
        pkt = IP(dst=hostname, ttl=i) / UDP(dport=33434)
        # Send the packet and get a reply
        reply = sr1(pkt, verbose=0)
        if reply is None:
            # No reply =(
            break
        elif reply.type == 3:
            # We've reached our destination
            print ("Done!", reply.src)
            break
        else:
            # We're in the middle somewhere
            print ("%d hops away: " % i , reply.src)