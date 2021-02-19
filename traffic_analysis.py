import csv


def main():
	summary = {}
	global SERVER
	SERVER = '10.24.36.177'
	data = read_csv('tms_prd_capture_2_12_21.csv')
	summarize_traffic(data, summary)



def summarize_traffic(data, summary):
	data = filter(lambda row: row['Source Address'] == SERVER or row['Destination Address'] == SERVER, data)

	port_summary = {}

	for row in data:
		src_port, dest_port = parse_int(row['Source Port']), parse_int(row['Destination Port'])
		service_ports = get_service_port(row)
		owning_proc = row['Process Filename']
		service_name = row['Service Name']
		packet_ct = parse_int(row['Packets Count'].replace(',', ''))
		outbound = row['Source Address'] == SERVER
		client_addr = row['Destination Address'] if outbound else row['Source Address']

		info = get_service_port(row)

		if info:
			if info['service_port'] in port_summary:
				port = info['service_port']
				if info['remote_service']:
					port_summary[port]['servers'].add(info['address'])
				else:
					port_summary[port]['clients'].add(info['address'])
				port_summary[port]['packet_count'] += packet_ct
				if owning_proc not in port_summary[port]['processes']:
					port_summary[port]['processes'].append(owning_proc)

			else:
				port_summary[info['service_port']] = {
					'service_name': row['Service Name'] if row['Service Name'] else 'Unknown',
					'clients': set() if info['remote_service'] else {info['address']},
					'servers': {info['address']} if info['remote_service'] else set(),
					'packet_count': packet_ct,
					'processes': [owning_proc]
				}

	print(sorted(port_summary.items()))

	dump_results(dict(sorted(port_summary.items())), 'results.txt')


def dump_results(results, filename):

	with open(filename, 'w') as f:
		for port, summary in results.items():
			clients_str = '\t' + '\n\t'.join(summary['clients'])  + '\n'
			servers_str = '\t' + '\n\t'.join(summary['servers']) + '\n'
			process_str = ', '.join(summary['processes']) + '\n'
			f.write('------------{} - {}-----------\n'.format(port, summary['service_name']))
			f.write('Packet Count: {}\n'.format(summary['packet_count']))
			f.write('Processes: {}'.format(process_str))
			f.write('CLIENT IPs\n')
			f.write(clients_str)
			f.write('SERVER IPs\n')
			f.write(servers_str)
			f.write('\n\n')




def is_client_port(port):
	SERVICE_PORT_MAX = 49151
	return port > SERVICE_PORT_MAX


def get_service_port(data):
	outbound = data['Source Address'] == SERVER
	src, dest = parse_int(data['Source Port']), parse_int(data['Destination Port'])

	port_info= {'remote_service': None, 'service_port': 0, 'address': None}
	service_ports = []
	# case where data is missing
	if src == 0 or dest == 0: return None 
	# unable to determine service port because they both in client range
	# if is_client_port(src) and is_client_port(dest): 
	# 	print('BOTH CLIENT PORTS')
	# 	print(src, dest)
	# 	return None


	port_info['address'] = data['Destination Address'] if outbound else data['Source Address']	# talking on same port 

	# src in client range 
	if is_client_port(src):
		port_info['remote_service'] = True if outbound else False
		port_info['service_port'] = dest 
	# dest in client range
	elif is_client_port(dest):
		port_info['remote_service'] = False if outbound else True
		port_info['service_port'] = src 
	# Talking on the same port 
	elif src == dest:
		port_info['remote_service'] = True
		port_info['service_port'] = src or dest	
	# Two service ports talking 
	else:
		print(src, dest)
		print('handling later...')

	return port_info



def parse_int(str):
	try:
		return int(str)
	except (TypeError, ValueError):
		return 0

def read_csv(filename):

	with open(filename) as csvfile:
		data = csv.DictReader(csvfile, delimiter=',')
		return list(data)

if __name__ == '__main__':
	main()
