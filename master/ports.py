datakeepers_ips = [
    "tcp://127.0.0.1:",
    "tcp://127.0.0.1:",
    "tcp://127.0.0.1:"
]

master_own_ip = "tcp://127.0.0.1:"

master_ports = [
    master_own_ip+"5500",
    master_own_ip+"5501",
    master_own_ip+"5502"
]

master_alive_port = "5400"


ports_per_datakeeper = [3, 0, 0]