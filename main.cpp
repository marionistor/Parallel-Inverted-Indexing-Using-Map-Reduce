#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <bits/stdc++.h>

#include "utils.h"

using namespace std;

// function for getting valid words from a line
vector<string> get_words_from_line(string line)
{
    string word;
    vector<string> words;

    for (char character : line) {
        if (!isspace(character)) {
            if (isalpha(character)) {
                word += tolower(character);
            }
        } else {
            if (!word.empty()) {
                words.push_back(word);
                word.clear();
            }
        }
    }

    if (!word.empty()) {
        words.push_back(word);
    }

    return words;
}

// function for mapper threads
void *mapper_function(void *argv)
{
    mapper *mapper_struct = (mapper *) (argv);

    while (1) {
        pthread_mutex_lock(&mapper_struct->common->mutex);

        /* each mapper thread extracts a file from the queue,
           parses it and puts the words in his partial_list */
        if (!mapper_struct->files->empty()) {
            string line;
            int file_id = mapper_struct->files->front().first;
            string file = mapper_struct->files->front().second;
            ifstream fin(file);

            if (!fin.is_open()) {
                fprintf(stderr, "Error opening file.\n");
                pthread_mutex_unlock(&mapper_struct->common->mutex);
                exit(1);
            }

            mapper_struct->files->pop();
            pthread_mutex_unlock(&mapper_struct->common->mutex);

            // read the file line by line and get words from it
            while (getline(fin, line)) {
                vector<string> words = get_words_from_line(line);

                for (string word : words) {
                    mapper_struct->partial_list[word].insert(file_id);
                }
            }

            fin.close();
        } else {
            pthread_mutex_unlock(&mapper_struct->common->mutex);
            break;
        }
    }

    pthread_barrier_wait(mapper_struct->common->barrier);

    return NULL;
}

// comparator for sorting the words that start with the same letter
bool comp(const pair<string, set<int>>& pair1, const pair<string, set<int>>& pair2)
{
    if (pair1.second.size() != pair2.second.size()) {
        return (pair1.second.size() > pair2.second.size());
    }

    return (pair1.first < pair2.first);
}

// function that extracts words that start with letter from all partial lists
vector<pair<string, set<int>>> get_words_by_first_letter(char letter, vector<mapper> *mapper_structs)
{
    unordered_map<string, set<int>> words_map;

    for (const mapper& m : *mapper_structs) {
        for (const auto& entry : m.partial_list) {
            if (entry.first[0] == letter) {
                words_map[entry.first].insert(entry.second.begin(), entry.second.end());
            }
        }
    }

    vector<pair<string, set<int>>> words(words_map.begin(), words_map.end());

    sort(words.begin(), words.end(), comp);

    return words;
}

// function for reducer threads
void *reducer_function(void *argv)
{
    reducer *reducer_struct = (reducer *) (argv);
    pthread_barrier_wait(reducer_struct->common->barrier);

    while (1) {
        pthread_mutex_lock(&reducer_struct->common->mutex);

        /* each reducer thread extracts a letter from the queue
           and then obtains a sorted list with all words from the
           partial lists that start with that letter and creates
           the file for that letter */
        if (!reducer_struct->letters->empty()) {
            char letter = reducer_struct->letters->front();
            ofstream fout(string(1, letter) + ".txt");

            if (!fout.is_open()) {
                fprintf(stderr, "Error opening file.\n");
                pthread_mutex_unlock(&reducer_struct->common->mutex);
                exit(1);
            }

            reducer_struct->letters->pop();
            pthread_mutex_unlock(&reducer_struct->common->mutex);

            vector<pair<string, set<int>>> words = get_words_by_first_letter(letter, reducer_struct->mapper_structs);

            for (pair<string, set<int>> pair : words) {
                fout << pair.first << ":[";

                for (auto it = pair.second.begin(); it != prev(pair.second.end()); it++) {
                    fout << *it << " ";
                }

                fout << *prev(pair.second.end()) << "]\n";
            }

            fout.close();
        } else {
            pthread_mutex_unlock(&reducer_struct->common->mutex);
            break;
        }
    }

    return NULL;
}

int main(int argc, char **argv)
{
    int nr_files, nr_mappers, nr_reducers, rc;

    if (argc != 4) {
        fprintf(stderr, "Usage: ./tema1 <nr_mappers> <nr_reducers> <input_file>\n");
        exit(1);
    }

    nr_mappers = atoi(argv[1]);
    nr_reducers = atoi(argv[2]);

    ifstream fin(argv[3]);

    if (!fin.is_open()) {
        fprintf(stderr, "Error opening file.\n");
        exit(1);
    }

    fin >> nr_files;
    fin.get();

    common_elements mapper_common, reducer_common;
    queue<pair<int, string>> files;
    queue<char> letters;

    // put all files that need to be processed by mapper threads
    for (int i = 1; i <= nr_files; i++) {
        string file;
        getline(fin, file);
        files.push({i, file});
    }

    fin.close();

    pthread_barrier_t barrier;
    pthread_t mapper_threads[nr_mappers], reducer_threads[nr_reducers];
    vector<mapper> mapper_structs(nr_mappers);

    rc = pthread_mutex_init(&mapper_common.mutex, NULL);
    DIE(rc != 0, "Error initializing mapper mutex.");

    rc = pthread_mutex_init(&reducer_common.mutex, NULL);
    DIE(rc != 0, "Error initializing reducer mutex.");

    /* barrier to ensure that reducer threads start working only
       after all mapper threads have finished */
    rc = pthread_barrier_init(&barrier, NULL, nr_mappers + nr_reducers);
    DIE(rc != 0, "Error initializing barrier.");

    mapper_common.barrier = &barrier;
    reducer_common.barrier = &barrier;

    // put all letters in the queue used by the reducer threads
    for (char letter = 'a'; letter <= 'z'; letter++) {
        letters.push(letter);
    }

    reducer reducer_struct;
    reducer_struct.common = &reducer_common;
    reducer_struct.mapper_structs = &mapper_structs;
    reducer_struct.letters = &letters;

    for (int i = 0; i < nr_mappers + nr_reducers; i++) {
        if (i < nr_mappers) {
            mapper_structs[i].common = &mapper_common;
            mapper_structs[i].files = &files;
            rc = pthread_create(&mapper_threads[i], NULL, mapper_function, &mapper_structs[i]);
            DIE(rc != 0, "Error creating mapper thread.");
        } else {
            rc = pthread_create(&reducer_threads[i - nr_mappers], NULL, reducer_function, &reducer_struct);
            DIE(rc != 0, "Error creating reducer thread.");
        }
    }

    for (int i = 0; i < nr_mappers + nr_reducers; i++) {
        if (i < nr_mappers) {
            rc = pthread_join(mapper_threads[i], NULL);
            DIE(rc != 0, "Error joining mapper threads.");
        } else {
            rc = pthread_join(reducer_threads[i - nr_mappers], NULL);
            DIE(rc != 0, "Error joining reducer threads.");
        }
    }
    
    pthread_mutex_destroy(&mapper_common.mutex);
    pthread_mutex_destroy(&reducer_common.mutex);
    pthread_barrier_destroy(&barrier);

    return 0;
}