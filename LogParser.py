import re
import os
import pandas as pd

path = 'MTBF'

columns_names=['Base Station', 'Cell ID', 'Date', 'Time', 'pmRrcConnEstabSucc', 'pmRrcConnEstabAtt', 'pmEndcRelUeNormal', 'pmEndcRelUeAbnormalSgnb', 'pmEndcRelUeAbnormalMenb', 'pmEndcRelUeAbnormalSgnbAct', 'pmEndcRelUeAbnormalMenbAct', 'pmPdcpLatTimeDl', 'pmPdcpLatPktTransDl', 'pmCellDowntimeAuto', 'pmTotalTimeDlCellCong', 'pmCellDowntimeMan', 'pmPdcpVolDlDrb']

df = pd.DataFrame(columns=columns_names)

def read_log_file(file_path):
    with open(file_path, 'r') as f:
        base_station = ''
        date = ''
        start_index = 0
        time_array = []
        cell_id = ''
        for line in f.readlines():
            try:
                if line.index('> run', 0, 20) > 0:
                    base_station = line[0:line.index('> run')]
                    df.at[len(df.index) + 1, 'Base Station'] = base_station
                    start_index = len(df.index)
            except Exception as e:
                pass
            try:
                if line.index('Date:', 0, 10) == 0:
                    date = line[6:]
                    df.at[len(df.index), 'Date'] = date
            except Exception as e:
                pass
            try:
                if line.index('Object ', 0, 10) == 0:
                    time_positions = re.finditer(':', line)
                    time_indices = [position.start() - 2 for position in time_positions]
                    for index in time_indices:
                        time = line[index : index + 5]
                        time_array.append(time)
                        if start_index == len(df.index):
                            df.at[start_index, 'Time'] = time
                            start_index -= 1
                        else:
                            df.at[len(df.index) + 1, 'Time'] = time
                            df.at[len(df.index), 'Base Station'] = base_station
                            df.at[len(df.index), 'Date'] = date
                    start_index += 1
            except Exception as e:
                pass
            if len(time_array) != 0:
                try:
                    if "=" in line:
                        id = line[line.index('=') + 1 : line.index(' ')]
                        if cell_id == '':
                            cell_id = id
                            for index in range(start_index, len(df.index) + 1):
                                df.at[index, 'Cell ID'] = cell_id
                        elif cell_id != id:
                            cell_id = id
                            end_index = len(df.index) + 1 + (len(df.index) - start_index) + 1
                            start_index = len(df.index) + 1
                            for index in range(start_index, end_index):
                                df.at[index, 'Cell ID'] = cell_id
                                df.at[index, 'Base Station'] = base_station
                                df.at[index, 'Date'] = date
                                df.at[index, 'Time'] = time_array[index - start_index]
                        for title in columns_names[4:]:
                            if title in line:
                                line = line[line.index(title):]
                                values = [int(s) for s in line.split() if s.isdigit()]
                                for index in range(start_index, len(df.index) + 1):
                                    df.at[index, title] = values[index - start_index]
                except Exception as e:
                    pass

for file in os.listdir(path):
    if file.endswith('.log'):
        file_path = f'{path}\{file}'
        read_log_file(file_path)

writer = pd.ExcelWriter('Data_Parse.xlsx', engine = 'xlsxwriter')
df.to_excel(writer, sheet_name = 'Data_Parse', index = False)
for column in df:
    column_width = max(df[column].astype(str).map(len).max(), len(column)) + 2
    column_width = min(column_width, 300)
    col_idx = df.columns.get_loc(column)
    writer.sheets['Data_Parse'].set_column(col_idx, col_idx, column_width)
writer.save()