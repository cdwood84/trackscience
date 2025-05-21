import os

def get_all_files_in_directory(root_directory):
    file_names = []
    if not os.path.isdir(root_directory):
        print(f"Error: The directory '{root_directory}' does not exist.")
        return file_names

    for dirpath, dirnames, filenames in os.walk(root_directory):
        for filename in filenames:
            file_names.append(filename)
    return file_names

def file_chunk(id_list, count=20, num=1):
    if count > 50:
        count = 50
    elif count < 2:
        count = 2
    if num < 1:
        num = 1
    elif (num - 1) * count > len(id_list):
        return None
    
    sub_string = ''
    for x in range((num - 1) * count, num * count):
        if x < len(id_list):
            if len(sub_string) > 0:
                sub_string += ', '
            sub_string += id_list[x]

    return sub_string

def get_started():
    files = get_all_files_in_directory('/Volumes/ENTERTHEMOX/Beatport')
    id_list = []
    id_string = ''
    for x in range(0,len(files)):
        # print('considering: '+files[x])
        if '-' in files[x] and ('.wav' in files[x] or '.aiff' in files[x]):
            id = files[x].split('-')[0]
            # print('  trying id: '+str(id))
            if id.isdigit():
                # print('  success')
                if len(id_string) > 0:
                    id_string += ', '
                id_string += str(id)
                id_list.append(id)
    print('Total number of tracks: ' + str(len(id_list)))
    print(file_chunk(id_list, 40,1))