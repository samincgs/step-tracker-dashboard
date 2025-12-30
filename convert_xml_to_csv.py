import re
import xml.etree.ElementTree as Xet
import pandas as pd
import time

def clean_device_name(device_str):
    if not device_str:
        return ''
    
    match = re.search(r'name:.*creation date:[^>]+', device_str)
    return match.group(0) if match else device_str

def main():
    xmlparse = Xet.parse('data/export.xml')
    root = xmlparse.getroot()
    
    step_data = []
    
    for record in root.findall('Record'):
        if record.get('type') == 'HKQuantityTypeIdentifierStepCount':
            current_step = {
                'start_date': record.get('startDate'),
                'end_date': record.get('endDate'),
                'value': int(record.get('value')),
                'source': record.get('sourceName'),
                'device': record.get('device')
            }
            step_data.append(current_step)
    
    df = pd.DataFrame(step_data)
    
    # cleaning the data before transforming it into a csv
    df['date'] = pd.to_datetime(df['start_date']).dt.date
    df['device'] = df['device'].apply(clean_device_name)
    
    
    daily_steps = df.groupby('date')['value'].sum().reset_index()
    daily_steps = daily_steps.rename(columns={'value': 'total_steps'})
    metadata = df.groupby('date')[['source', 'device']].first().reset_index()

    
    daily_steps = pd.merge(daily_steps, metadata, 'left')
    
    daily_steps.to_csv('data/step_counts.csv', index=False)
    
    print('Finished')

if __name__ == '__main__':
    main()
