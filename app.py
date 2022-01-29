import pymongo
import pandas as pd
import json 
import matplotlib.pyplot as plt

client = pymongo.MongoClient("mongodb+srv://adithya:password_omitted@cluster0.lfxug.mongodb.net/")
db = client['aat']

sample = {
    'location': 'Zimbabwe', 
    'date': '2021-11-01', 
    'variant': 'others', 
    'num_sequences': 0, 
    'perc_sequences': 0.0, 
    'num_sequences_total': 6
}

while True:
    print("\nSelect option: ")
    print("1. Import database\n2. Create record\n3. Read\n4. Update records\n5. Delete records\
\n6. Graph query\n7. Exit")
    ch = int(input())
    
    if ch == 1: 
        data = pd.read_csv('covid-variants.csv')
        payload = json.loads(data.to_json(orient='records'))
        db.variants.drop()
        db.variants.insert_many(payload)
        print("Successfully imported.")
        
    elif ch == 2:
        cpy = sample.copy()
        for key in cpy:
            cpy[key] = input(f'Enter {key}: ')
        db.variants.insert_one(cpy)
        
    elif ch == 3:
        query = input('Enter parameters (all to find all): \n')
        try:
            if query != 'all':
                count = 0
                for x in db.variants.find(eval(query)):
                    count += 1
                    for key in x:
                        print(f'{key}: {x[key]}', end = '|')
                    print('\n')
                print(f'{count} docunments found.')
                
            else:
                count = 0
                for x in db.variants.find():
                    count += 1
                    for key in x:
                        print(f'{key}: {x[key]}', end = '|')
                    print('\n')
                print(f'{count} docunments found.')
        except:
            print('Incorrect query')
            
    elif ch == 4:
        query = input('Enter parameters: \n')
        new_values = { "$set": eval(input('Enter new values: \n')) }
        try:
            x = db.variants.update_many(eval(query), new_values)
            print(x.modified_count, "documents updated.")
        except:
            print('Incorrect query')
            
    elif ch == 5:
        query = input('Enter parameters: \n')
        try:
            x = db.variants.delete_many(eval(query))
            print(x.deleted_count, " documents deleted.")
        except:
            print('Incorrect query')
            
    elif ch == 6:
        country = input('Select country: ')
        variant1 = input('Select first variant: ')
        variant2 = input('Select second variant: ')
        from_date = input('Select onwards date (YYYY-MM-DD): ')
        try:
            query = { '$or' : [ {'variant': variant1 }, {'variant':variant2}],  'location': country, 'date' : { '$gt' : from_date }}
            # for x in db.variants.find(query):
            #     print(x)
            df = pd.DataFrame(db.variants.find(query))
            # print(df)
            fig, ax = plt.subplots(figsize = (50, 10))
            for variant in df['variant'].unique():
                line =  df[df['variant'] == variant].groupby('date')['num_sequences'].agg('sum')
                ax.plot(line, label=variant)
                
            ax.legend()
            plt.ylabel('num_sequences processing')
            plt.xlabel('date')
            plt.show()
        except:
            print('Incorrect query.')
    else:
        print("Program ending.")
        break
    