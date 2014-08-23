#ifndef STATISTICS_
#define STATISTICS_
#include "ParseTree.h"
#include <string>
#include <map>
#include <vector>
#include <set>
#include <cstdlib>


using namespace std;

// Relation Information is described
struct DataOfRelation {
	unsigned long long int mTotalRows;

	// DataStructure for Attribute lookup <attrname, no of distinct values>
	map <string, unsigned long long int> mAttribute;

	// Relation partition, -1 represents the singleton situtation
	int mSelfPartition;
	
	DataOfRelation() {
		mTotalRows=0;
		mSelfPartition=-1;
	}
};



class Statistics
{

private:
	int mPartitionNum;
	map <string, DataOfRelation> mRelData;
	map <string, vector <string> > mColOfRelation;
	map <int, vector<string> > mPartitionInfo;
	struct ColumnCountAndEstimate
    {
        int repeatCount;         
        long double estimate;    
    };

    bool validateParseTree(struct AndList *parseTree, char *relNames[], int numToJoin, vector<string>&, set<string>&);
public:
	Statistics();
	Statistics(Statistics &copyMe);	 // Performs deep copy
	~Statistics();


	void AddRel(char *relName, int numTuples);
	void AddAtt(char *relName, char *attName, int numDistincts);
	void CopyRel(char *oldName, char *newName);

	void Read(char *fromWhere);
	void Write(char *fromWhere);

	void  Apply(struct AndList *parseTree, char *relNames[], int numToJoin);
	double Estimate(struct AndList *parseTree, char **relNames, int numToJoin);

	map<int, vector<string> > * GetPartitionInfo();
	map<string, DataOfRelation> * GetStatistics();
	int GetPartitionNum();
	map<string, vector<string> >* GetColumnRelation();

};

#endif
