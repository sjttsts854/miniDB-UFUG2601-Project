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
        while (command.find(';') != string::npos)
        {
            size_t pos = command.find(';');
            string sub = command.substr(0, pos);
            parseCommand(sub, db, outputFile);
            command = command.substr(pos + 1);
        }
    }

    input.close();
    return 0;
}