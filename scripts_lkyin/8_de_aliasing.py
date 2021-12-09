import pandas as pd
from jellyfish import damerau_levenshtein_distance
from jellyfish import jaro_similarity
from jellyfish import jaro_winkler_similarity
from jellyfish import match_rating_comparison
from collections import defaultdict
from tqdm import tqdm
import re
import json

def indices_dict(lis):
    d = defaultdict(list)
    for i,(a,b) in enumerate(lis):
        d[a].append(i)
        d[b].append(i)
    return d

def disjoint_indices(lis):
    d = indices_dict(lis)
    sets = []
    while len(d):
        que = set(d.popitem()[1])
        ind = set()
        while len(que):
            ind |= que 
            que = set([y for i in que 
                         for x in lis[i] 
                         for y in d.pop(x, [])]) - ind
        sets += [ind]
    return sets

# union-find algo
def disjoint_sets(lis):
    return [list(set([x for i in s for x in lis[i]])) for s in disjoint_indices(lis)]


def process_name(name):
    # if it is an email, take only the user domain
    name = name.split('@')[0]
    # remove text within brakets and parentheses
    name = re.sub(r"[\(\[].*?[\)\]]", "", name)
    # remove non-alphanumeric chars
    name = re.sub("[^a-zA-Z ]+", '', name)
    if '$' in name:
        # some names are of this pattern: "sg $ $date: 2008/10/07 10:18:51 $"
        name = name.split('$')[0]
    return name.strip()


def check_segments(name1, name2):

    name_segs_1 = name1.split(' ')
    name_segs_2 = name2.split(' ')

    if len(name_segs_1) == len(name_segs_2) == 2:
        first_name_1, last_name_1 = name_segs_1
        first_name_2, last_name_2 = name_segs_2

        # option 1: first name 1 compare to first name 2, last name 1 compare to last name 2
        # e.g., "robert yates" v.s. "robert butts"
        first_name_score = jaro_winkler_similarity(first_name_1, first_name_2)
        last_name_score = jaro_winkler_similarity(last_name_1, last_name_2)
        if first_name_score < 0.8 or last_name_score < 0.8:
            return False
        # option 2: first name 1 compare to last name 2, last name 1 compare to first name 2
        # e.g., "yates robert" v.s. "robert butts"
        else:
            first_name_score = jaro_winkler_similarity(first_name_1, last_name_2)
            last_name_score = jaro_winkler_similarity(last_name_1, first_name_2)
            if first_name_score < 0.8 or last_name_score < 0.8:
                return False
    return True


print('reading csv...')
commits_df = pd.read_csv('./commits_preprocessed.csv', usecols=['project_name', 'author_full_name', 'is_bot', 'is_coding'])
emails_df = pd.read_csv('./emails_preprocessed.csv', usecols=['project_name', 'author_full_name', 'is_bot'])
commits_df.query('is_bot == False and is_coding == True', inplace=True)
emails_df.query('is_bot == False', inplace=True)

print('saving alias csv...')
commits_df.dropna().to_csv('commit_alias.csv', index=False, columns=['project_name', 'author_full_name'])
emails_df.dropna().to_csv('email_alias.csv', index=False, columns=['project_name', 'author_full_name'])


commits_df = pd.read_csv('./commit_alias.csv')
emails_df = pd.read_csv('./email_alias.csv')

print('processing...')
commits_dict = commits_df.to_dict('records')
emails_dict = emails_df.to_dict('records')


committers = {}
contributors = {}

for commit in commits_dict:
    project_name = commit['project_name']
    author_full_name = commit['author_full_name']
    if project_name not in committers:
        committers[project_name] = set()
    committers[project_name].add(author_full_name)

for email in emails_dict:
    project_name = email['project_name']
    author_full_name = email['author_full_name']
    if project_name not in contributors:
        contributors[project_name] = set()
    contributors[project_name].add(author_full_name)

# get projects set
c_projects = set(committers.keys())
e_projects = set(contributors.keys())
projects = sorted([p for p in c_projects.intersection(e_projects) if not pd.isna(p)])
# print(len(c_projects), len(e_projects))

project_alias_clustering = {}
for project in projects:
    clustering_pairs = []
    committer_names = set(committers[project])
    contributor_names = set(contributors[project])
    developer_names = list(committer_names.union(contributor_names))
    for i in range(len(developer_names)):
        p1 = process_name(developer_names[i])
        
        for j in range(i+1, len(developer_names)):
            # if it is an email, take only the user domain
            p2 = process_name(developer_names[j])

            # reslove issues that two different devs sharing same first name, 
            # e.g., "robert ottaway", "robert sayre"
            if not check_segments(p1, p2):
                continue

            jaro_winkler_similarity_score = jaro_winkler_similarity(p1, p2)
            # sounding_match_score = any([match_rating_comparison(s1, s2) for s1 in name_segs_1 for s2 in name_segs_2])
            # sounding_match_score = any([sounding_match_score, match_rating_comparison(p1, p2)])

            # add pairs if:
            # (1) if the score fall down to 0.85 
            # (2) or if the score fall down to 0.82 then we use pronouncetion to help make decision
            if jaro_winkler_similarity_score > 0.85: # or (jaro_winkler_similarity_score > 0.82 and sounding_match_score):
                clustering_pairs.append([developer_names[i], developer_names[j]])
    
    project_alias_clustering[project] = disjoint_sets(clustering_pairs)

with open('./project_alias_clustering.json', 'w') as f:
    json.dump(project_alias_clustering, f, indent = 4)

with open('./project_alias_clustering.json', 'r') as f:
    project_alias_clustering = json.load(f)

# post-processing
print('starting post-processing...')
for project in project_alias_clustering:
    # print(project, len(project_alias_clustering[project]))
    for n_index, n_cluster in enumerate(project_alias_clustering[project]):
        cluster = n_cluster[:]
        lowest_score = 0
        # continue checking if the avg score of any nodes is below 0.85
        while len(cluster) >= 2 and lowest_score < 0.85:
            name_to_pop = None
            lowest_score = float('inf')
            score_dict = {}
            for name_i in cluster:
                p1 = process_name(name_i)
                this_score = 0
                for name_j in cluster:
                    if name_i == name_j: continue
                    p2 = process_name(name_j)
                    jaro_winkler_similarity_score = jaro_winkler_similarity(p1, p2)
                    this_score += jaro_winkler_similarity_score/(len(cluster)-1)
                if this_score < lowest_score:
                    name_to_pop = name_i
                    lowest_score = this_score
                score_dict[name_i] = this_score

            if lowest_score < 0.85:
                cluster.pop(cluster.index(name_to_pop))
                score_dict.pop(name_to_pop)

        # continue checking segments in names of two-parts form, e.g., "robert ottaway", "robert sayre"
        flag = True
        while flag and cluster:
            flag = False
            # use cluster_copy to avoid affecting the for-loop
            pop_set = set()
            for name_i in cluster:
                p1 = process_name(name_i)
                for name_j in cluster:
                    if name_i == name_j: continue
                    p2 = process_name(name_j)
                    # if the two names cant be in same cluster
                    # pop the node with lowest avg. score
                    if check_segments(p1, p2):
                        continue
                    flag = True
                    if score_dict[name_i] < score_dict[name_j]:
                        pop_set.add(name_i)
                    else:
                        pop_set.add(name_j)

            for name in pop_set:
                cluster.pop(cluster.index(name))
                score_dict.pop(name)

        # ingore large cluster or if it has fewer than two names in the cluster
        if len(cluster) < 2 or len(cluster) > 5:
            project_alias_clustering[project].pop(n_index)
            continue

        # manual check: cases that dont make sense
        elif 'michael glauche' in cluster and 'michael akerman' in cluster:
            project_alias_clustering[project].pop(n_index)
            continue
        elif 'martin kool' in cluster and 'martin vojtek' in cluster:
            project_alias_clustering[project].pop(n_index)
            continue
        elif 'john arnold' in cluster and 'john hofman' in cluster:
            project_alias_clustering[project].pop(n_index)
            continue
        elif 'martin von gagern' in cluster and 'martin weber' in cluster:
            project_alias_clustering[project].pop(n_index)
            continue
        else:
            project_alias_clustering[project][n_index] = cluster

with open('./project_alias_clustering_filtered.json', 'w') as f:
    json.dump(project_alias_clustering, f, indent = 4)

# construct the alias to full name mapping
# ideally, we can find a "regular" full name for each cluster. 
# this does not tend to affect the result of de-aliasing, it is only for a better looking 
# (e.g., "gerrie" will be mapped to "gerrie myburgh" but not "gerriem")
alias_mapping = {}
for project in project_alias_clustering:
    if project not in alias_mapping:
        alias_mapping[project] = []
    
    for alias_list in project_alias_clustering[project]:
        longest_name = None 
        longest_name_length = float('-inf')
        list_of_regular_names = []

        for alias in alias_list:
            length = len(alias)
            if length > longest_name_length:
                longest_name = alias 
                longest_name_length = length

            # exactly has two parts, which is a regular full name
            if len(alias.split(' ')) == 2:
                list_of_regular_names.append([alias, length])

        # first piority: longest name with two exactly parts (i.e., first name, last name)
        if list_of_regular_names:
            final_name = sorted(list_of_regular_names, key=lambda x: x[1]).pop()[0]
        # second piority: the longest name in the list
        else: final_name = longest_name
        alias_mapping[project].append({alias: final_name for alias in alias_list if alias!=final_name})

with open('./alias_mapping.json', 'w') as f:
    json.dump(alias_mapping, f, indent = 4)



def de_alising(project_name, author_name):
    if (project_name in alias_mapping) and (author_name in alias_mapping[project_name]):
        return alias_mapping[project_name][author_name]
    return author_name


print('reading commit csv...')
df = pd.read_csv('./commits_preprocessed.csv')
df['dealised_author_full_name'] = df.apply(lambda x: de_alising(x['project_name'], x['author_full_name']), axis=1)
df['dealised_author_full_name'] = df.apply(lambda x: de_alising(x['project_name'], x['author_full_name']), axis=1)
print('writing final commit csv...')
df.to_csv('commits_final.csv', index=False)


print('reading email csv...')
df = pd.read_csv('./emails_preprocessed.csv')
df['dealised_author_full_name'] = df.apply(lambda x: de_alising(x['project_name'], x['author_full_name']), axis=1)
df['dealised_author_full_name'] = df.apply(lambda x: de_alising(x['project_name'], x['author_full_name']), axis=1)
print('writing final email csv...')
df.to_csv('./emails_final.csv', index=False)

print('All done!')

'''
# to fix pattern: 


['michael glauche', 'michael akerman', 'michael mogley', 'michael rudolf', 'michael voigt', 'michel']



fixed pattern 10/19/2021:

#
"martin ronner",
"martyh",
"martin goulet",
"uma_rk",
"mail",
"mari",
"marisg",
"martin",
"marasm",
"martinzan",
"martin strohal",
"mar1394"

#
"robert yates",
"robert butts",
"robert ottaway",
"robert sayre"

#
"gerrie myburgh",
"gerrie myburg  [ mtn - innovation centre ]"

'''
