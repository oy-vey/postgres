// If you choose to use C++, read this very carefully:
// https://www.postgresql.org/docs/15/xfunc-c.html#EXTEND-CPP

#include "dog.h"
#include "jsmn.h"

// clang-format off
extern "C" {
#include "../../../../src/include/postgres.h"
#include "../../../../src/include/fmgr.h"
#include "../../../../src/include/foreign/fdwapi.h"
#include "../../../../src/include/foreign/foreign.h"
#include "../../../../src/include/optimizer/pathnode.h"
#include "../../../../src/include/optimizer/planmain.h"
#include "../../../../src/include/utils/builtins.h"
#include "../../../../src/include/access/sdir.h"
#include "../../../../src/include/access/stratnum.h"
#include "../../../../src/include/lib/stringinfo.h"
#include "../../../../src/include/nodes/bitmapset.h"
#include "../../../../src/include/nodes/lockoptions.h"
#include "../../../../src/include/nodes/parsenodes.h"
#include "../../../../src/include/nodes/primnodes.h"
#include "../../../../src/include/nodes/nodeFuncs.h"
#include "../../../../src/include/nodes/execnodes.h"

#include "../../../../src/include/utils/elog.h"
#include "../../../../src/include/utils/typcache.h"
#include "../../../../src/include/utils/lsyscache.h"

#include "../../../../src/include/optimizer/restrictinfo.h"


}
// clang-format on

#define MAX_KEYS 128 * 10
#define MAX_COLUMNS 128 * 10

int numRowsToRead;
int rowsRead;
char *filename;
int column_count;
char * file_data[MAX_COLUMNS];
int coltypes[MAX_COLUMNS]; // 0 - str, 1 - int, 2 - float
int coltypesizes[MAX_COLUMNS];
int colnumblocks[MAX_COLUMNS];
int colnumrecords[MAX_COLUMNS];
int coloffsets[MAX_COLUMNS];



static int jsoneq(const char *json, jsmntok_t *tok, const char *s) {
  if (tok->type == JSMN_STRING && (int)strlen(s) == tok->end - tok->start &&
      strncmp(json + tok->start, s, tok->end - tok->start) == 0) {
    return 0;
  }
  return -1;
}


// Function to read K bytes from a specific offset from either the start or end of a file
char* readKBytes(const char* filename, long k, long offset, int fromEnd) {
    // Open the file in binary mode for reading
    FILE* file = fopen(filename, "rb");
    // if (file == NULL) {
    //     elog(LOG, "Error opening file");
    //     return NULL;
    // }

    // Determine the total size of the file
    fseek(file, 0, SEEK_END); // Move to the end of the file
    long file_size = ftell(file); // Get the file size
    // elog(LOG, "File size: %ld", file_size);
    // if (file_size == -1) {
    //     elog(LOG, "Error getting file size");
    //     fclose(file);
    //     return NULL;
    // }

    // Calculate the position to read from
    long position;
    if (fromEnd) {
        // Reading from the end
        position = file_size - k - offset;
        if (position < 0) {
            position = 0; // Ensure we don't seek before the start of the file
        }
    } else {
        // Reading from the start
        position = offset;
        if (position > file_size) {
            position = file_size; // Ensure we don't seek beyond the end of the file
        }
    }
    // elog(LOG, "Position: %ld, Offset: %ld", position, offset);

    // Seek to the desired position
    fseek(file, position, SEEK_SET);

    // Allocate memory for the buffer
    char* buffer = (char*)malloc(k);
    if (buffer == NULL) {
        // elog(LOG, "Error allocating memory");
        fclose(file);
        return NULL;
    }

    // Read K bytes into the buffer
    size_t bytes_read = fread(buffer, 1, k, file);
    if (bytes_read == 0) {
        // elog(LOG, "Error reading file");
        free(buffer);
        fclose(file);
        return NULL;
    }

    // Close the file
    fclose(file);

    return buffer;
}

int json_print_tokens(const char* json) {
    jsmn_parser parser;
    jsmntok_t tokens[128 * 10]; // Adjust the token array size as needed
    jsmn_init(&parser);

    // Parse the JSON string into tokens
    jsmn_parse(&parser, json, strlen(json), tokens, sizeof(tokens) / sizeof(tokens[0]));

    // if (num_tokens < 0) {
    //     fprintf(stderr, "JSON parsing error: %d\n", num_tokens);
    //     return -1;
    // }

    // Iterate through the tokens to find the key
    // for (int i = 1; i < num_tokens; i++) {
    //  elog(LOG, "Size: %i, Start:%i, End: %i", tokens[i].size, tokens[i].start, tokens[i].end);
    // }

    return 1;
}


// Function to extract a JSON value given a key
int json_get_value(const char* json, const char* key, char* value, int max_len) {
    jsmn_parser parser;
    jsmntok_t tokens[128 * 10]; // Adjust the token array size as needed
    jsmn_init(&parser);

    // Parse the JSON string into tokens
    int num_tokens = jsmn_parse(&parser, json, strlen(json), tokens, sizeof(tokens) / sizeof(tokens[0]));

    // if (num_tokens < 0) {
    //     fprintf(stderr, "JSON parsing error: %d\n", num_tokens);
    //     return -1;
    // }

    // Iterate through the tokens to find the key
    for (int i = 1; i < num_tokens; i++) {
        if (jsoneq(json, &tokens[i], key) == 0) {
            int len = tokens[i + 1].end - tokens[i + 1].start;
            // Ensure the value fits in the provided buffer
            if (len < max_len) {
                strncpy(value, json + tokens[i + 1].start, len);
                value[len] = '\0'; // Null-terminate the string
                return 0; // Success
            } else {
                // fprintf(stderr, "Value is too long for the buffer\n");
                return -1;
            }
        }
    }

    // fprintf(stderr, "Key not found in JSON\n");
    return -1; // Key not found
}

// Function to extract a list of keys from JSON
int json_get_keys(const char* json, char* keys[], int max_keys) {
    jsmn_parser parser;
    jsmntok_t tokens[MAX_KEYS]; // Use a maximum number of tokens to avoid reallocating

    jsmn_init(&parser);
    int num_tokens = jsmn_parse(&parser, json, strlen(json), tokens, MAX_KEYS);

    if (num_tokens < 0) {
        // fprintf(stderr, "JSON parsing error: %d\n", num_tokens);
        return -1;
    }

    if (num_tokens < 1 || tokens[0].type != JSMN_OBJECT) {
        // fprintf(stderr, "Top-level JSON data is not an object\n");
        return -1;
    }

    int key_count = 0;
    int cur_ptr = 0;

    for (int i = 1; i < num_tokens; i += 2) {
        if(tokens[i].start <= cur_ptr) {
            continue;
        }
        if (tokens[i + 1].size > 0) {
            cur_ptr = tokens[i + 1].end;
        }

        int len = tokens[i].end - tokens[i].start;
        if (key_count < max_keys) {
            keys[key_count] = (char*)malloc(len + 1);
            strncpy(keys[key_count], json + tokens[i].start, len);
            keys[key_count][len] = '\0';
            key_count++;
        } else {
            // fprintf(stderr, "Maximum number of keys reached\n");
            break;
        }
    }

    return key_count;
}


char* reverseAtoi(int num) {
    // Handle the special case of num = 0
    if (num == 0) {
        return strdup("0");
    }

    // Determine the number of digits in the integer
    int temp = num;
    int numDigits = 0;
    while (temp != 0) {
        temp /= 10;
        numDigits++;
    }

    // Allocate memory for the string representation
    char* str = (char*)malloc((numDigits + 1) * sizeof(char));
    if (str == NULL) {
        return NULL; // Memory allocation failed
    }

    // Convert the integer to a string in reverse order
    char* ptr = str + numDigits;
    *ptr = '\0';
    while (num != 0) {
        ptr--;
        *ptr = '0' + (num % 10);
        num /= 10;
    }

    return str;
}


char* getElements(char* array, int offset, int k) {
    if (offset < 0 || k <= 0) {
        return NULL; // Invalid input parameters
    }

    // Allocate memory for the result array
    char* result = (char*)malloc(k * sizeof(char));
    if (result == NULL) {
        return NULL; // Memory allocation failed
    }

    // Copy the elements from the original array to the result array
    for (int i = 0; i < k; i++) {
        result[i] = array[offset + i];
    }

    return result;
}

extern "C" void db721_GetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel,
                                      Oid foreigntableid) {

  /* Get the ForeignTable from the foreigntableid */
  ForeignTable *foreign_table = GetForeignTable(foreigntableid);
  List *options = foreign_table->options;
  
  // FILE *file;
  // char buffer[4];

  // Iterate through the list to access the filename
  ListCell *cell;
  foreach(cell, options) {
      DefElem *def = (DefElem *)lfirst(cell);
      if (strcmp(def->defname, "filename") == 0) {
        filename = strVal(def->arg);
      }
  }
  


}
extern "C" void db721_GetForeignPaths(PlannerInfo *root, RelOptInfo *baserel,
                                    Oid foreigntableid) {
  add_path(baserel, (Path *)create_foreignscan_path(root, baserel, 
                            NULL, 
                            baserel->rows, 
                            100, 
                            100, 
                            NIL, 
                            baserel->lateral_relids,
                            NULL,
                            NIL));
}

extern "C" ForeignScan *
db721_GetForeignPlan(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid,
                   ForeignPath *best_path, List *tlist, List *scan_clauses,
                   Plan *outer_plan) {    
    scan_clauses = extract_actual_clauses(scan_clauses, false);   
    ForeignScan *fscan = make_foreignscan(tlist, scan_clauses, baserel->relid, NIL, NIL, NIL, NULL, outer_plan);
//    elog(LOG, "Node Type: %s", nodeToString(fscan));


    // ListCell *lc;
    // foreach (lc, scan_clauses) {
    //     Expr *clause = (Expr *)lfirst(lc);
    //     translate_clause_to_filter(clause);
    //     elog(LOG, "GetForeignPlan clause: %s", nodeToString(clause));
    // }



   return fscan;    
}

extern "C" void db721_BeginForeignScan(ForeignScanState *node, int eflags) {
    // Read last 4 bytes to get jsonMetadataSize 
    char * buffer = readKBytes(filename, 4, 0, 1);
    int jsonMetadataSize = (int)((unsigned char)buffer[0] |
                    ((unsigned char)buffer[1] << 8) |
                    ((unsigned char)buffer[2] << 16) |
                    ((unsigned char)buffer[3] << 24));
   // elog(LOG, "BIGINNING FOREIGN SCAN");

    // Read jsonMetadataSize bytes to get jsonMetadata 
    buffer = readKBytes(filename, jsonMetadataSize, 4, 1);
    char * jsonMetadata = buffer;
    jsonMetadata[jsonMetadataSize] = '\0';
   // elog(LOG, "jsonMetadata: %s", jsonMetadata);

    // Extract column metadata from jsonMetadata
    char columnMetadata[128 * 1000];
    json_get_value(jsonMetadata, "Columns", columnMetadata, sizeof(columnMetadata));
    // columnMetadata[128 * 1000 + 1] = '\0';
    // elog(LOG, "Column metadata: %s", columnMetadata);

    char max_values_per_block_buffer[100];
    json_get_value(jsonMetadata, "Max Values Per Block", max_values_per_block_buffer, sizeof(max_values_per_block_buffer));
    int max_values_per_block = atoi(max_values_per_block_buffer);
    
    
    // Extract list of columns from columnMetadata
    char* columns[128 * 100]; // Array to store columns, adjust the size as needed
    column_count = json_get_keys(columnMetadata, columns, sizeof(columns));
    // if (column_count > 0) {
    //         printf("List of columns:\n");
    //         for (int i = 0; i < column_count; i++) {
    //             printf("%d: %s\n", i + 1, columns[i]);
    //         }
    //     } else {
    //        elog(LOG, "No columns found in JSON.\n");

    // }

    //int max_col_size = 0;
    // int cur_col_size = 0;
    char cur_col_metadata_buffer[128 * 1000];
    char cur_col_blockstats_buffer[128 * 1000];
    char cur_col_numblocks_buffer[32];
    char cur_col_offset_buffer[10000];
    char cur_col_type_buffer[32];
    char cur_col_last_block_buffer[1000];
    char cur_col_last_block_nums_buffer[1000];
   
    for (int i = 0; i < column_count; i++) {

        json_get_value(columnMetadata, columns[i], cur_col_metadata_buffer, sizeof(cur_col_metadata_buffer));
       // elog(LOG, "cur_col_metadata_buffer %s", cur_col_metadata_buffer);
        json_get_value(cur_col_metadata_buffer, "num_blocks", cur_col_numblocks_buffer, sizeof(cur_col_numblocks_buffer));
       // elog(LOG, "cur_col_numblocks_buffer %s", cur_col_numblocks_buffer);
        colnumblocks[i] = atoi(cur_col_numblocks_buffer);
        json_get_value(cur_col_metadata_buffer, "start_offset", cur_col_offset_buffer, sizeof(cur_col_offset_buffer));
       // elog(LOG, "cur_col_offset_buffer %s", cur_col_offset_buffer);
        coloffsets[i] = atoi(cur_col_offset_buffer);
        json_get_value(cur_col_metadata_buffer, "block_stats", cur_col_blockstats_buffer, sizeof(cur_col_blockstats_buffer));
       // elog(LOG, "cur_col_blockstats_buffer %s", cur_col_blockstats_buffer);
        json_get_value(cur_col_metadata_buffer, "type", cur_col_type_buffer, sizeof(cur_col_type_buffer));
       // elog(LOG, "cur_col_type_buffer %s", cur_col_type_buffer);
        if(strcmp(cur_col_type_buffer, "str") == 0){
            coltypes[i] = 0;
            coltypesizes[i] = 32;
        }
        else if (strcmp(cur_col_type_buffer, "int") == 0){
            coltypes[i] = 1;
            coltypesizes[i] = 4;
        }
        else if (strcmp(cur_col_type_buffer, "float") == 0){
            coltypes[i] = 2;
            coltypesizes[i] = 4;
        }
        
        json_get_value(cur_col_metadata_buffer, reverseAtoi(colnumblocks[i] - 1), cur_col_last_block_buffer, sizeof(cur_col_last_block_buffer));
       // elog(LOG, "cur_col_last_block_buffer %s", cur_col_last_block_buffer);
        json_get_value(cur_col_last_block_buffer, "num", cur_col_last_block_nums_buffer, sizeof(cur_col_last_block_nums_buffer));
       // elog(LOG, "cur_col_last_block_nums_buffer %s", cur_col_last_block_nums_buffer);

        colnumrecords[i] = (colnumblocks[i] - 1) * max_values_per_block + atoi(cur_col_last_block_nums_buffer);
    }
    

    //elog(LOG, "max_col_size: %i", max_col_size);
    
    for (int i = 0; i < column_count; i++) {
        file_data[i] = readKBytes(filename,  colnumrecords[i] * coltypesizes[i], coloffsets[i], 0);
    }
   // elog(LOG, "Successfully read the file");
    rowsRead = 0;





    // TODO:
    // free(columns[i]); // Free the allocated memory for each key
    

}

extern "C" TupleTableSlot *db721_IterateForeignScan(ForeignScanState *node) {

    // elog(LOG, "ITERATING FOREIGN SCAN");

    TupleTableSlot * slot = node->ss.ss_ScanTupleSlot;
    if (rowsRead == colnumrecords[0]) {
       // elog(LOG, "No More rows");
        return NULL;
    }

    for (int i = 0; i < column_count; i++) {
        //elog(LOG, "copying to buffer");
        
        char* buffer = getElements(file_data[i], rowsRead * coltypesizes[i], coltypesizes[i]);
        //elog(LOG, "success copying to buffer");
        ExecClearTuple(slot);
        if (coltypes[i] == 0) {
            slot->tts_values[i] = CStringGetTextDatum(buffer);
            slot->tts_isnull[i] = false;
        }
        else if (coltypes[i] == 1) {
            int intData = (int)((unsigned char)buffer[0] |
                ((unsigned char)buffer[1] << 8) |
                ((unsigned char)buffer[2] << 16) |
                ((unsigned char)buffer[3] << 24));
            slot->tts_values[i] = Int32GetDatum(intData);
            // slot->tts_isnull[i] = true;
        }
        else if (coltypes[i] == 2) {
            union {
                char bytes[4];
                float floatValue;
            } data;
            
            for (int i = 0; i < 4; i++) {
                data.bytes[i] = buffer[i];
            }
            slot->tts_values[i] = Float4GetDatum(data.floatValue);
            //slot->tts_isnull[i] = true;
        }

    }
   // elog(LOG, "rowsRead: %i/%i", rowsRead, colnumrecords[0]);
    ExecStoreVirtualTuple(slot);
    rowsRead++;
    return slot;

}

extern "C" void db721_ReScanForeignScan(ForeignScanState *node) {
  // TODO(721): Write me!
}

extern "C" void db721_EndForeignScan(ForeignScanState *node) {
  // TODO(721): Write me!
}