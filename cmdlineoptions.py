import sys, getopt

def commandlineOptions(name, argv):

    ## defaults
    no_nodes = int("1,000".replace(",",""))
    no_rels = int("5,000".replace(",",""))
    records_per_file = int("20,000,000".replace(",",""))
    database = 'social'
    processes = 6
    output_format = "parquet" ## or CSV

    try:
        opts, args = getopt.getopt(argv,"hsd:u:f:r:p:o:",["database=","users=","follow_rels=","records=","processes=","output="])
    except getopt.GetoptError:
        print (name + ' -d <database> -p <processes_to_use> -u <no_users> -f <no_follow_rels> -r <no_records_per_file> -o <output_format>')
        sys.exit(2)

    for opt, arg in opts:
        if opt == '-h':
            print (name + ' -d <database> -p <processes_to_use> -u <no_users> -f <no_follow_rels> -r <no_records_per_file> -o <output_format>')
            sys.exit()
        elif opt in ("-d", "--database"):
            database = arg
        elif opt in ("-u", "--users"):
            no_nodes = int(arg)
        elif opt in ("-f", "--follow_rels"):
            no_rels = int(arg)
        elif opt in ("-r", "--records"):
            records_per_file = int(arg)
        elif opt in ("-p", "--processes"):
            processes = int(arg)
        elif opt in ("-o", "--output"):
            output_format = arg

        elif opt == '-s':
            print ('** Test Mode')
            no_nodes = int("1,000".replace(",",""))
            no_rels = int("50,000".replace(",",""))
            records_per_file = int("200,000".replace(",",""))
            database = 'social'


    print ('Number Concurrent Processes: ' + str(processes))
    print ('Database name for import script: ' + database)
    print ('Number nodes to generate: ' + str(no_nodes))
    print ('Number rels to generate: ' + str(no_rels))
    print ('Number records per file: ' + str(records_per_file))
    print ('Output format: ' + str(output_format))


    return database, no_nodes, no_rels, records_per_file, processes, output_format
