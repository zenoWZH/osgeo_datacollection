import pandas as pd
import json
import re

# map alias to real name. return full name if name exists in dict
def partial_de_alising(author_name):
    return str(alias_dict.get(author_name, author_name)).lower().strip()

def preprocess_name(name):
    # remove nan
    if pd.isna(name):
        return name
    # if it is an email, take only the user domain
    name = name.split('@')[0]
    # remove text within brakets and parentheses
    name = re.sub(r"[\(\[].*?[\)\]]", "", name)
    # some names are of this pattern: "sg $ $date: 2008/10/07 10:18:51 $"
    if '$' in name:
        name = name.split('$')[0]
    # remove non-alphanumeric chars
    name = re.sub("[^a-zA-Z ]+", '', name)
    if ' via ' in name:
        name = name.split(' via ')[0]
    return name.strip()

# return True if it is sent by a bot
def is_bots(author_name):
    return bool(author_name in bots)

# return True if it is a coding file
def is_coding(file_path):
    if pd.isna(file_path):
        return False
    extension = '.' + file_path.split('/')[-1].split('.')[-1]
    return bool(extension in coding_extensions)

with open('./email_bots.txt', 'r') as f:
    bots = set(f.read().splitlines())

with open('./Programming_Languages_Extensions.json', 'r') as f:
    programming_languages_extensions = json.load(f)

coding_extensions = set(['.mdtext'])
for pl in programming_languages_extensions:
    if 'extensions' not in pl:
        continue
    # filter out some data extensions, e.g., json
    if pl['type'] != 'programming' and pl['type'] != 'markup':
        continue
    coding_extensions = coding_extensions.union(set(pl['extensions']))


with open('./public_ldap_people.json', 'r') as f:
    alias_to_name = json.load(f)

alias_dict = {}
for alias in alias_to_name['people']:
    alias_dict[alias] = alias_to_name['people'][alias]['name']

print('processing commits...')
commit_df = pd.read_csv('./commits_full.csv')
commit_df['author_full_name'] = commit_df['author_name'].apply(lambda x: preprocess_name(x))
commit_df['author_full_name'] = commit_df['author_full_name'].apply(lambda x: partial_de_alising(x))
commit_df['is_bot'] = commit_df['author_name'].apply(lambda x: is_bots(x))
commit_df['is_coding'] = commit_df['file_name'].apply(lambda x: is_coding(x))
commit_df.to_csv('./commits_preprocessed.csv', index=False)


print('processing emails...')
email_df = pd.read_csv('./emails_full.csv')
#test_df = email_df[email_df.project_name == 'tvm']
#test_df.to_csv('./test_df.csv', index=False)
# email_df = pd.read_csv('test_df.csv', usecols=['date','sender_name'])
email_df['author_full_name'] = email_df['sender_name'].apply(lambda x: preprocess_name(x))
email_df['author_full_name'] = email_df['author_full_name'].apply(lambda x: partial_de_alising(x))
email_df['is_bot'] = email_df['sender_name'].apply(lambda x: is_bots(x))
email_df.to_csv('./emails_preprocessed.csv', index=False)
