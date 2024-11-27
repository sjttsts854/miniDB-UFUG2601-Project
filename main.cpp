#include "miniDB.h"
#include <iostream>
#include <fstream>
#include <sstream>

using namespace std;

int main(int argc, char* argv[]) 
{
    if (argc != 3) 
    {
        cerr << "Usage: " << argv[0] << " <input SQL file> <output file>" << endl;
        return 1;
    }

    string inputFile = argv[1];
    string outputFile = argv[2];

    miniDB db;

    ifstream input(inputFile);
    if (!input.is_open()) 
    {
        cerr << "Error: Could not open input file: " << inputFile << endl;
        return 1;
    }

    string command;
    string line;

    while (getline(input, line)) 
    {
        // 去除行首和行尾的空白字符
        line.erase(0, line.find_first_not_of(" \t\n\r"));
        line.erase(line.find_last_not_of(" \t\n\r") + 1);

        if (line.empty()) 
        {
            continue;
        }

        command += line + " ";
        if (line.find(';') != string::npos) 
        {
            parseCommand(command, db, outputFile);
            command.clear();
        }
    }

    input.close();
    return 0;
}