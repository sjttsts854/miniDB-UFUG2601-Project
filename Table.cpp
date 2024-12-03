#include "Table.h"
#include <fstream>
#include <iostream>
#include <sstream>
#include <iomanip>


using namespace std;

Table::Table(const string& name, const vector<string>& columns, const vector<string>& columnTypes)
    : name(name), columns(columns), columnTypes(columnTypes) {}

void Table::addRow(const vector<string>& row) 
{
    data.push_back(row);
}



void Table::saveToCSV(const string& outputFile) const 
{
    ofstream file(outputFile);
    if (!file.is_open()) 
    {
        cerr << "Error: Could not open file for writing: " << outputFile << endl;
        return;
    }

    // 写入列名
    for (size_t i = 0; i < columns.size(); ++i) 
    {
        file << columns[i];
        if (i < columns.size() - 1) 
        {
            file << ",";
        }
    }
    file << endl;

    // 写入数据行
    for (const auto& row : data) 
    {
        for (size_t i = 0; i < row.size(); ++i) 
        {
            if (columnTypes[i] == "TEXT") 
            {
                file << row[i] ;
            } 
            else if (columnTypes[i] == "FLOAT") 
            {
                file << fixed << setprecision(2) << stof(row[i]);
            } 
            else 
            {
                file << row[i];
            }
            if (i < row.size() - 1) 
            {
                file << ",";
            }
        }
        file << endl;
    }

    file.close();
}