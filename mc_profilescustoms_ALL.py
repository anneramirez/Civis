### PROFILES Loop through pages to get all results ###
obj = 'profile'
def loopPages(url,auth,params): 
    startTime= datetime.datetime.now()
    rPro = []
    rCus = []
    while True: #change to while True when done testing!!
        try:
            resp = getAPIdata(url,auth,params)
            tree = processXML(resp)
            path = tree['response'][obj+'s'][obj]
            customs = process_sublist(path,'custom_column')
            rCus.extend(customs)
            clean = cleanPro(path)
            r = flatXML(clean)
            rPro.extend(r)
            params['page'] += 1 #go to next page    
            if params['page']%50 == 0: #evaluate current page
                pushData(rPro,rCus)
                rPro = []
                rCus = []
            if params['page']%10 == 0:
                timeElapsed=datetime.datetime.now()-startTime
                print("Processed " + str(params['page']) + " pages in " + str(timeElapsed))

            #else:
             #   break
        except Exception as ex:
            print(ex)
            break
    params['page'] = 1
    pushData(rPro,rCus)
