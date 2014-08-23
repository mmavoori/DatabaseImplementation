#include "Statistics.h"
#include <iostream>
#include <fstream>
#include <sstream>
#include <cstring>
#include <set>
#include <cstdlib>
using namespace std;

Statistics::Statistics(){
	mPartitionNum=0;
}
Statistics::~Statistics()
{}
// Perform a deep copy of the Statistics Object
Statistics::Statistics(Statistics &copyMe)
{
	mPartitionNum = copyMe.GetPartitionNum();
	// Perform a deep copy of the Relation Information.
	map<string,DataOfRelation> * lCopyMe = copyMe.GetStatistics();
	map <string,DataOfRelation>::iterator itr;
	map <string, unsigned long long int >::iterator atts_itr;
	for (itr=lCopyMe->begin(); itr!=lCopyMe->end();itr++){
		DataOfRelation lRelData;
		lRelData.mSelfPartition =itr->second.mSelfPartition;
		lRelData.mTotalRows =itr->second.mTotalRows;		
		
		for (atts_itr=itr->second.mAttribute.begin(); atts_itr!=itr->second.mAttribute.end(); ++atts_itr) {
			lRelData.mAttribute[atts_itr->first] = atts_itr->second;
		}		
		mRelData[itr->first] = lRelData;
	}

	// Perform a deep copy of Partition Information
	map <int, vector<string> > * lPartitionInfo = copyMe.GetPartitionInfo();
	map <int, vector<string> >::iterator p_itr;
	for ( p_itr = lPartitionInfo->begin(); p_itr != lPartitionInfo->end(); ++p_itr) {
		vector<string> lRelNames;		
		vector<string> * copyMeRelNames = &p_itr->second;
		for (int i = 0; i < copyMeRelNames->size(); i++) {
			lRelNames.push_back(copyMeRelNames->at(i));
		}
		
		mPartitionInfo[p_itr->first] = lRelNames;
	}

	// Perform a deep copy of Column Relation
	map <string, vector <string> > * lColOfRelation = copyMe.GetColumnRelation();
	map <string, vector <string> >::iterator lColIter;
	for ( lColIter = lColOfRelation->begin(); lColIter != lColOfRelation->end(); ++lColIter){
		std::vector<string> lTemp;
		vector<string> * lRefVector = &lColIter->second;
		for (int i = 0; i < lRefVector->size(); i++) {
			lTemp.push_back(lRefVector->at(i));
		}
		
		mColOfRelation[lColIter->first] = lTemp;	
	}
}

map<int, vector<string> >* Statistics::GetPartitionInfo()
{
    return &mPartitionInfo;
}
	
map<string, DataOfRelation>* Statistics::GetStatistics()
{
    return &mRelData;
}

map<string, vector<string> >* Statistics::GetColumnRelation()
{
    return &mColOfRelation;
}
	
int Statistics::GetPartitionNum()
{
    return mPartitionNum;
}

void Statistics::AddRel(char *relName, int numTuples)
{
	int lcount=mRelData.count(string(relName));
	//if not found create a new one and add
	if (lcount==0) {
		DataOfRelation lRelData;
		lRelData.mTotalRows = numTuples;
		mRelData[string(relName)] = lRelData;
	}
	else {  //find the relname and update its numTuples
		map <string, DataOfRelation>::iterator itr = mRelData.find(string(relName));
		itr->second.mTotalRows = numTuples;
	}
}
void Statistics::AddAtt(char *relName, char *attName, int numDistincts)
{
	int lcount=mRelData.count(string(relName));
    if (lcount==0) {
        DataOfRelation lRelData;
        //Set Attribute data
		lRelData.mAttribute[string(attName)] = numDistincts;
        mRelData[string(relName)] = lRelData;
		// Include data to column Relation
		vector<string> lRelationName;
		lRelationName.push_back(string(relName));
		mColOfRelation[string(attName)] = lRelationName;
    }
    else {
    	map <string, DataOfRelation>::iterator itr = mRelData.find(string(relName));
        // if relation is existing already get its attribute data to find for existing attribute
        int lcount=(itr->second).mAttribute.count(string(attName));

		// if attribute not existing, create one and add it
		if (lcount==0) {
			(itr->second).mAttribute[string(attName)] = numDistincts;
			// Include data to column relation
			vector<string> lRelationName;
	        lRelationName.push_back(string(relName));
	        mColOfRelation[string(attName)] = lRelationName;
		}
		else{	// update the distinct count
			map <string, unsigned long long int>::iterator att_itr = (itr->second).mAttribute.find(string(attName));
			att_itr->second = numDistincts;
		}
    }
}
void Statistics::CopyRel(char *oldName, char *newName)
{
	//Search for the old relation name, whether it exists
	if (mRelData.count(string(oldName))!=0)
	{
	map <string, DataOfRelation>::iterator itr = mRelData.find(string(oldName));
	//Get the attribute data of the old relation name
	DataOfRelation *lOldRelData = &(itr->second);
	DataOfRelation lRelData;
	lRelData.mTotalRows = lOldRelData->mTotalRows;
	map <string, vector<string> > :: iterator col_itr;
	map <string, unsigned long long int>::iterator atts_itr;
	for (atts_itr = lOldRelData->mAttribute.begin(); atts_itr != lOldRelData->mAttribute.end(); atts_itr++) {
		lRelData.mAttribute[atts_itr->first] = atts_itr->second;
		//Search for column in the relation
		col_itr = mColOfRelation.find(atts_itr->first);
		//if column name not found
		if (col_itr == mColOfRelation.end()) {
				cout << "Column name " << (atts_itr->first).c_str() << " is not found....."<<endl;
				return;
		}
		//add New Relation name to the ColumnRelation Data structure for that column
		(col_itr->second).push_back(string(newName));
	}
	//Add attribute data to the relation
	mRelData[string(newName)] = lRelData;
	}
	else{
		cout<<"No Relation with name " <<oldName <<"exists..."<<endl;
	}

}

void Statistics::Read(char *fromWhere)
{
       char line[500];
    string lRelNames, lAttrNames;
    unsigned long long int lDistinctValues;
    FILE * fp = fopen(fromWhere, "r");
    if (fp == NULL)
        return; 
    map <string, vector <string> >::iterator itr;
    while (fscanf(fp, "%s", line) != EOF) {
        if( strcmp(line, "BEGIN") == 0 ) {
            fscanf(fp, "%s", line);
            lRelNames = line;            
            DataOfRelation lRelData;
            fscanf(fp, "%llu", &lRelData.mTotalRows);            
            fscanf(fp, "%s", line);
            while (strcmp(line, "END") != 0)
            {
                lAttrNames = line;
                fscanf(fp, "%llu", &lDistinctValues);
                lRelData.mAttribute[lAttrNames] = lDistinctValues;
                itr = mColOfRelation.find(string(lAttrNames));
                //Attribute name not found, create new one.
                if (itr == mColOfRelation.end()) { 
                    vector<string> tableName;
                    tableName.push_back(string(lRelNames));
                    mColOfRelation[string(lAttrNames)] = tableName;
                }
                else {
                    (itr->second).push_back(string(lRelNames));
                }                                
                fscanf(fp, "%s", line);
            }
            mRelData[lRelNames] = lRelData;
        }
    }   

}
void Statistics::Write(char *fromWhere)
{
  ofstream file;
  file.open (fromWhere);
  map <string, DataOfRelation>::iterator itr;
	for (itr=mRelData.begin(); itr != mRelData.end(); itr++)
	{
		file << "BEGIN\n";
		file << itr->first.c_str() <<endl;
		DataOfRelation * lRelData = &(itr->second);
		file << lRelData->mTotalRows <<endl;
		map <string, unsigned long long int>::iterator atts_itr;
		for (atts_itr=lRelData->mAttribute.begin(); atts_itr != lRelData->mAttribute.end(); atts_itr++) {
			file << atts_itr->first.c_str() << endl;
			file << atts_itr->second << endl;
		}
		file << "END\n\n";
	}
  file.close();
}

void  Statistics::Apply(struct AndList *parseTree, char *relNames[], int numToJoin)
{
    double lRowCount = Estimate(parseTree, relNames, numToJoin);
    if (lRowCount == -1) {
        cout << "Estimation error"<<endl;
        return;
    }
    else {
        vector<string> lJoinAttributes;
        set <string> lJoinTableSet;
        if (!validateParseTree(parseTree, relNames, numToJoin, lJoinAttributes, lJoinTableSet)) {
            cout<< "Not a valid parse tree... check parseTree"<<endl;
            return;
        }
        else {
            int lOldPartitionNo = -1;
            set <string>::iterator lSetIter = lJoinTableSet.begin();
            map <string, DataOfRelation>::iterator itr;
            
            for (; lSetIter != lJoinTableSet.end(); ++lSetIter) {
                itr = mRelData.find(*lSetIter);
                if (itr == mRelData.end()) {
                    cout << "Details of table " << (*lSetIter).c_str() << " is not found...."<<endl;
                    return;
                }   
                if ((itr->second).mSelfPartition != -1) {
                    lOldPartitionNo = (itr->second).mSelfPartition;
                    break;
                }
            }

            if (lOldPartitionNo == -1) {
                mPartitionNum++;
                vector <string> lTableNames;
                for (lSetIter = lJoinTableSet.begin(); lSetIter != lJoinTableSet.end(); ++lSetIter)
                {
                    itr = mRelData.find(*lSetIter);
                    (itr->second).mSelfPartition = mPartitionNum;
                    (itr->second).mTotalRows = (unsigned long long int)lRowCount;
                    lTableNames.push_back(*lSetIter);
                }
                mPartitionInfo[mPartitionNum] = lTableNames;
            }
            else {
                vector <string> lTableNames = mPartitionInfo[lOldPartitionNo];
                for (int i=0; i<lTableNames.size(); i++)
                    lJoinTableSet.insert(lTableNames.at(i));
                lTableNames.clear();
                for (lSetIter = lJoinTableSet.begin(); lSetIter != lJoinTableSet.end(); ++lSetIter)
                {
                    itr = mRelData.find(*lSetIter);
                    (itr->second).mSelfPartition = lOldPartitionNo;
                    (itr->second).mTotalRows = (unsigned long long int)lRowCount;
                    lTableNames.push_back(*lSetIter);
                }
                mPartitionInfo[lOldPartitionNo] = lTableNames;
            }
        }
    }
}

double Statistics::Estimate(struct AndList *parseTree, char **relNames, int numToJoin)
{
    set <string> lTemp;
    vector<string> joinAttsInPair;
    if ( ! validateParseTree(parseTree, relNames, numToJoin, joinAttsInPair, lTemp)) {
        cout<< "Invalid parseTree..." << endl;
        return -1;
    }
    
    set <string> lJoinTableSet;
    vector<long double> estimates;
    map<string, ColumnCountAndEstimate> localOrListEstimates;
    string last_connector = "";
    int i = 0;
    while(i < joinAttsInPair.size())
    {
        long double localEstimate = -1;
        int col1Type = atoi(joinAttsInPair.at(i++).c_str());
        string col1Val = joinAttsInPair.at(i++);
        int operatorCode = atoi(joinAttsInPair.at(i++).c_str());
        int col2Type = atoi(joinAttsInPair.at(i++).c_str());
        string col2Val = joinAttsInPair.at(i++);
        string connector = joinAttsInPair.at(i++);
        string lTable1;
        int prefixedTabNamePos;
        if(col1Type == NAME) {
            prefixedTabNamePos = col1Val.find(".");
            if(prefixedTabNamePos != string::npos){
                lTable1 = col1Val.substr(0, prefixedTabNamePos);
                col1Val = col1Val.substr(prefixedTabNamePos + 1);
            }
            else
                lTable1 = mColOfRelation[col1Val].at(0);

            lJoinTableSet.insert(lTable1);
        }

        string lTable2;
        if(col2Type == NAME)
        {
            prefixedTabNamePos = col2Val.find(".");
            if(prefixedTabNamePos != string::npos)
            {
                lTable2 = col2Val.substr(0, prefixedTabNamePos);
                col2Val = col2Val.substr(prefixedTabNamePos + 1);
            }
            else
                lTable2 = mColOfRelation[col2Val].at(0);

            lJoinTableSet.insert(lTable2);
        }

        if(col1Type == NAME && col2Type == NAME)    //join condition
        {
            DataOfRelation t1;
            DataOfRelation t2;
            //find localEstimate
            t1 = mRelData[lTable1];
            t2 = mRelData[lTable2];

            localEstimate = 1.0/(max(t1.mAttribute[col1Val], t2.mAttribute[col2Val]));

            estimates.push_back(localEstimate);
        }
        else if(col1Type == NAME || col2Type == NAME)
        {
            DataOfRelation t;
            string colName;
            if(col1Type == NAME)
            {
                t = mRelData[lTable1];
                colName = col1Val;
            }
            else
            {
                t = mRelData[lTable2];
                colName = col2Val;
            }
            if(operatorCode == EQUALS)
            {
                if(connector.compare("OR") == 0 || last_connector.compare("OR") == 0)
                {
                    if(localOrListEstimates.find(colName + "=") == localOrListEstimates.end())
                    {
                        localEstimate = (1.0- 1.0/t.mAttribute[colName]);
                        ColumnCountAndEstimate *cce = new ColumnCountAndEstimate();
                        cce->repeatCount = 1;
                        cce->estimate = localEstimate;
                        localOrListEstimates[colName + "="] = *cce;
                    }
                    else    // we did find some column name repeated with = sign
                    {
                        localEstimate = 1.0/t.mAttribute[colName];
                        ColumnCountAndEstimate* cce = &(localOrListEstimates[colName + "="]);
                        cce->repeatCount += 1;
                        cce->estimate = cce->repeatCount*localEstimate;
                    }
                    if(connector.compare("OR") != 0)
                    {
                        long double tempResult = 1.0;
                        map<string, ColumnCountAndEstimate>::iterator it = localOrListEstimates.begin();
                        for(; it != localOrListEstimates.end(); it++)
                        {
                            if(it->second.repeatCount == 1)
                                tempResult *= it->second.estimate;
                            else
                                tempResult *= (1 - it->second.estimate);
                        }

                        long double totalCurrentEstimate = 1.0 - tempResult;
                        estimates.push_back(totalCurrentEstimate);

                        localOrListEstimates.clear();                        
                    }
                }
                else
                {
                    localEstimate = 1.0/t.mAttribute[colName];
                    estimates.push_back(localEstimate);
                }
            }
            else    // operator is either > or <
            {
                if(connector.compare("OR") == 0 || last_connector.compare("OR") == 0)
                {
                    localEstimate = (1.0 - 1.0/3);
                    
                    ColumnCountAndEstimate *cce = new ColumnCountAndEstimate();
                    cce->repeatCount = 1;
                    cce->estimate = localEstimate;
                    localOrListEstimates[colName] = *cce;

                    if(connector.compare("OR") != 0)
                    {
                        long double tempResult = 1.0;
                        map<string, ColumnCountAndEstimate>::iterator it = localOrListEstimates.begin();
                        for(; it != localOrListEstimates.end(); it++)
                        {
                            if(it->second.repeatCount == 1)
                                tempResult *= it->second.estimate;
                            else
                                tempResult *= (1 - it->second.estimate);
                        }

                        long double totalCurrentEstimate = 1.0 - tempResult;
                        estimates.push_back(totalCurrentEstimate);
                        
                        localOrListEstimates.clear();
                    }
                }
                else
                {
                    localEstimate = (1.0/3);
                    estimates.push_back(localEstimate);
                }
            }
        }
        last_connector = connector;
    }
    
    //compute the numerator
    unsigned long long int numerator = 1;
    set <string>::iterator lSetIter = lJoinTableSet.begin();
    for (; lSetIter != lJoinTableSet.end(); ++lSetIter)
        numerator *= mRelData[*lSetIter].mTotalRows;

    double result = numerator;
    for(int i = 0; i < estimates.size(); i++)
    {
        result *= estimates.at(i);
    }
    return result;
}

// Validate the parseTree
bool Statistics::validateParseTree(struct AndList *parseTreeArg, char *relNames[], int numToJoin, vector<string>& joinAttsInPair, set<string>& table_names_set)
{
    for (int j = 0; j < numToJoin; j++) {
        if(mRelData.find(relNames[j]) == mRelData.end())
            return false;
    }

    int prefixedTabNamePos;
    string sTableName, sColName;
    map<string, vector<string> >::iterator c2t_itr;

    AndList* parseTree = parseTreeArg;
    
    while(parseTree != NULL)
    {
        OrList* orList = parseTree->left;
        while(orList != NULL)
        {
            ComparisonOp* comparisonOp = orList->left;
            if(comparisonOp == NULL) {
                break;
            }
            
            //first push the left value
            int leftCode = comparisonOp->left->code;
            string leftVal = comparisonOp->left->value;
            
            stringstream ss1;
            ss1 << leftCode;
        
            joinAttsInPair.push_back(ss1.str());
            joinAttsInPair.push_back(leftVal);

            //now push the operator
            stringstream ss2;
            ss2 << comparisonOp->code;
            joinAttsInPair.push_back(ss2.str());

            //and now the right value
            int rightCode = comparisonOp->right->code;
            string rightVal = comparisonOp->right->value;
            
            stringstream ss3;
            ss3 << rightCode;
            joinAttsInPair.push_back(ss3.str());
            joinAttsInPair.push_back(rightVal);

            if(leftCode == NAME)    //check if column belongs to some table or not
            {
                //also check if the column name has "table_name." prefixed with it
                prefixedTabNamePos = leftVal.find(".");
                if (prefixedTabNamePos != string::npos)     // table_name.col_name format
                {
                    sTableName = leftVal.substr(0, prefixedTabNamePos);
                    sColName = leftVal.substr(prefixedTabNamePos + 1);
                    c2t_itr = mColOfRelation.find(sColName);
                    if (c2t_itr == mColOfRelation.end())    // column not found
                        return false;
                }
                else
                {
                    sColName = leftVal;
                    c2t_itr = mColOfRelation.find(sColName);
                    if (c2t_itr == mColOfRelation.end())    // column not found
                        return false;
                        
                    if ((c2t_itr->second).size() > 1)
                        return false;   // ambiguity error!
                    else
                        sTableName = (c2t_itr->second).at(0);
                }
                table_names_set.insert(sTableName);
            }
            if(rightCode == NAME)   //check if column belongs to some table or not
            {
                prefixedTabNamePos = rightVal.find(".");
                if (prefixedTabNamePos != string::npos)
                {
                    sTableName = rightVal.substr(0, prefixedTabNamePos);
                    sColName = rightVal.substr(prefixedTabNamePos + 1);
                    c2t_itr = mColOfRelation.find(sColName);
                    if (c2t_itr == mColOfRelation.end())
                        return false;
                }
                else
                {
                    sColName = rightVal;
                    c2t_itr = mColOfRelation.find(sColName);
                    if (c2t_itr == mColOfRelation.end())
                        return false;

                    if ((c2t_itr->second).size() > 1)
                        return false;
                    else
                        sTableName = (c2t_itr->second).at(0);
                }
                table_names_set.insert(sTableName);
            }

            //move to next orList inside first AND
            if(orList->rightOr != NULL)
                joinAttsInPair.push_back("OR");
            orList = orList->rightOr;
        }
        //move to next AndList node of the parseTree
        if(parseTree->rightAnd != NULL)
            joinAttsInPair.push_back("AND");
        else
            joinAttsInPair.push_back(".");
        parseTree = parseTree->rightAnd;
    }
    
    // Now check if the tables being joined (from ParseTree) makes sense
    // with respect to the PartitionsInfo   
    for (int i=0; i<numToJoin; i++)
    {
        sTableName = relNames[i];
        int partitionNum = mRelData[sTableName].mSelfPartition;
        if (partitionNum != -1)
        {
            vector<string> v_tbl_names = mPartitionInfo[partitionNum];
            for (int j = 0; j < v_tbl_names.size(); j++)
            {
                string table1 = v_tbl_names.at(j);
                bool found = false;
                for (int k = 0; k < numToJoin; k++)
                {
                    string table2 = relNames[k];
                    if (table1.compare(table2) == 0)
                    {
                        found = true;
                        break;
                    }
                }   
                if (found == false)
                    return false;
            }
        }
    }

    set <string>::iterator lSetIter = table_names_set.begin();
    for (; lSetIter != table_names_set.end(); ++lSetIter)
    {
        string table1 = *lSetIter;
        bool found = false;
        for (int k = 0; k < numToJoin; k++)
        {
            string table2 = relNames[k];
            if (table1.compare(table2) == 0) {   
                found = true;
                break;
            }
        }
        if (found == false)
            return false;
    }

    return true;
}
