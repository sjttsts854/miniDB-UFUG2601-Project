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

void Table::saveToFile(const string& outputFile) const 
{
    ofstream file(outputFile);
    if (!file.is_open()) 
    {
        cerr << "Error: Could not open file for writing: " << outputFile << endl;
        return;
    }

    // 写入列名和列类型
    for (size_t i = 0; i < columns.size(); ++i) 
    {
        file << columns[i] << " " << columnTypes[i] << " ";
    }
    file << endl;

    // 写入数据行
    for (const auto& row : data) 
    {
        for (const auto& cell : row) 
        {
            file << cell << " ";
        }
        file << endl;
    }

    file.close();
}

void Table::loadFromFile(const string& inputFile) 
{
    ifstream file(inputFile);
    if (!file.is_open()) 
    {
        cerr << "Error: Could not open file for reading: " << inputFile << endl;
        return;
    }

    // 读取列名和列类型
    string line;
    getline(file, line);
    istringstream iss(line);
    columns.clear();
    columnTypes.clear();
    string col, colType;
    while (iss >> col >> colType) 
    {
        columns.push_back(col);
        columnTypes.push_back(colType);
    }

    // 读取数据行
    data.clear();
    while (getline(file, line)) 
    {
        istringstream iss(line);
        vector<string> row;
        string cell;
        while (iss >> cell) 
        {
            row.push_back(cell);
        }
        data.push_back(row);
    }

    file.close();
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
                file << "\"" << row[i] << "\"";
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