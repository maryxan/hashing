#ifndef HT_H_
#define HT_H_

extern int primarydesc;

typedef struct HT_info {
	int fileDesc;
	char attrType;
	char* attrName;
	int attrLength;
	long int numBuckets;
} HT_info;

typedef struct Record {
	int id;
	char name[15];
	char surname[20];
	char address[25];
} Record;

int HT_CreateIndex(char *filename, char attrType, char* attrName, int attrLength, int buckets);
HT_info* HT_OpenIndex( char *fileName);
int HT_CloseIndex( HT_info* header_info );
int HT_InsertEntry( HT_info header_info, Record record);
int HT_DeleteEntry( HT_info header_info, void *value );
int HT_GetAllEntries( HT_info header_info, void *value);

#endif /* HT_H_ */