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

}
// clang-format on

int num_rows_to_read;
int rows_read;
char *filename;



static int jsoneq(const char *json, jsmntok_t *tok, const char *s) {
  if (tok->type == JSMN_STRING && (int)strlen(s) == tok->end - tok->start &&
      strncmp(json + tok->start, s, tok->end - tok->start) == 0) {
    return 0;
  }
  return -1;
}




extern "C" void db721_GetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel,
                                      Oid foreigntableid) {

  /* Get the ForeignTable from the foreigntableid */
  ForeignTable *foreign_table = GetForeignTable(foreigntableid);
  List *options = foreign_table->options;
  
  FILE *file;
  char buffer[4];

  // Iterate through the list to access the filename
  ListCell *cell;
  foreach(cell, options) {
      DefElem *def = (DefElem *)lfirst(cell);
      if (strcmp(def->defname, "filename") == 0) {
        filename = strVal(def->arg);
      }
  }
  elog(LOG, "filename: %s", filename);
  file = fopen(filename, "rb");
  // Seek to the end of the file
  fseek(file, -4, SEEK_END);
  // Read the last 4 bytes into the buffer
  fread(buffer, 1, sizeof(buffer), file);
  int json_metadata = (int)((unsigned char)buffer[0] |
                ((unsigned char)buffer[1] << 8) |
                ((unsigned char)buffer[2] << 16) |
                ((unsigned char)buffer[3] << 24));
  elog(LOG, "buffer: %i", json_metadata);
  fseek(file, -4 - json_metadata, SEEK_END);
  char new_buffer[json_metadata];
  fread(new_buffer, 1, sizeof(new_buffer), file);
  elog(LOG, "new_buffer: %s", new_buffer);


  int r;
  jsmn_parser p;
  jsmntok_t t[256]; /* We expect no more than 256 tokens */

  jsmn_init(&p);
  r = jsmn_parse(&p, new_buffer, strlen(new_buffer), t,
                 sizeof(t) / sizeof(t[0]));

  // if (r < 0) {
  //   elog(LOG, "Failed to parse JSON: %d\n", r);
  // }

  // /* Assume the top-level element is an object */
  // if (r < 1 || t[0].type != JSMN_OBJECT) {
  //   elog(LOG, "Object expected\n");
  // }

  /* Loop over all keys of the root object */
  for (int i = 1; i < r; i++) {
    if (jsoneq(new_buffer, &t[i], "Table") == 0) {
      /* We may use strndup() to fetch string value */
      elog(LOG, "Table:");
      elog(LOG, "- Table: %.*s\n", t[i + 1].end - t[i + 1].start,
             new_buffer + t[i + 1].start);
      i++;
    }
  }

  fclose(file);



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
  return make_foreignscan(tlist, scan_clauses, baserel->relid, NIL, NIL, NIL,
                          NIL, outer_plan);
}

extern "C" void db721_BeginForeignScan(ForeignScanState *node, int eflags) {
  num_rows_to_read = 10; // Example: Set num_rows_to_read to 10
  rows_read = 0;
  elog(LOG, "totalRows: %i", num_rows_to_read);
}

extern "C" TupleTableSlot *db721_IterateForeignScan(ForeignScanState *node) {

  TupleTableSlot * slot = node->ss.ss_ScanTupleSlot;
  
  if (rows_read == num_rows_to_read) {
    elog(LOG, "No More rows");
    return NULL;
  }
  else {
    ExecClearTuple(slot);
    slot->tts_values[0] = CStringGetDatum("col1");
    slot->tts_isnull[0] = false;
    slot->tts_values[1] = (Datum)-999.0;
    slot->tts_isnull[1] = true;
    slot->tts_values[2] =(Datum)-888.0;
    slot->tts_isnull[2] = false;
    // node->ss.ss_currentScanDesc++;
    ExecStoreVirtualTuple(slot);
    elog(LOG, "Returning row %i/%i", rows_read, num_rows_to_read);
    rows_read++;
    return slot;

  }

}

extern "C" void db721_ReScanForeignScan(ForeignScanState *node) {
  // TODO(721): Write me!
}

extern "C" void db721_EndForeignScan(ForeignScanState *node) {
  // TODO(721): Write me!
}