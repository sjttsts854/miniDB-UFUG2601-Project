#include "Table.h"
#include <fstream>
#include <iostream>
#include <sstream>
using namespace std;

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

    // 写入列名
    for (const auto& col : columns) 
    {
        file << col << " ";
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

    // 读取列名
    string line;
    getline(file, line);
    istringstream iss(line);
    columns.clear();
    string col;
    while (iss >> col) 
    {
        columns.push_back(col);
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