#include "miniDB.h"
#include <iostream>
#include <fstream>
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
    while (getline(input, command)) 
    {
        parseCommand(command, db, outputFile);
    }

    input.close();
    return 0;
}