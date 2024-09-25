#include <iostream>
#include <fstream>
#include <string>
#include <sstream>
#include <nlohmann/json.hpp>
#include <unordered_map>
#include <vector>
#include <thread>
#include <mutex>

using namespace std;
using json = nlohmann::json;

struct BookNode {
    string isbn;
    string name;
    string author;
    string category;
    double price;
    int quantity;

    BookNode(string isbn, string name, string author, string category, double price, int quantity)
        : isbn(isbn), name(name), author(author), category(category), price(price), quantity(quantity) {}
};

void to_json(json& j, const BookNode& book) {
    j = json{ {"isbn", book.isbn}, {"name", book.name}, {"author", book.author}, {"category", book.category}, {"price", book.price}, {"quantity", book.quantity} };
}

class BookInventory {
private:
    unordered_map<string, BookNode> inventory;
    mutex mtx; // Mutex for thread safety

public:
    void insertBook(const string& isbn, const string& name, const string& author, const string& category, double price, int quantity) {
        lock_guard<mutex> lock(mtx);
        inventory[isbn] = BookNode(isbn, name, author, category, price, quantity);
    }

    void loadOperations(const string& fileName) {
        ifstream file(fileName);
        string content((istreambuf_iterator<char>(file)), (istreambuf_iterator<char>()));
        file.close();

        stringstream ss(content);
        string line;

        while (getline(ss, line)) {
            stringstream lineStream(line);
            string operation, jsonStr;
            getline(lineStream, operation, ';');
            getline(lineStream, jsonStr, ';');

            json jsonData;
            try {
                jsonData = json::parse(jsonStr);
            }
            catch (json::parse_error& e) {
                cerr << "Error al parsear el JSON: " << e.what() << endl;
                continue;
            }

            if (operation == "INSERT") {
                string isbn = jsonData.value("isbn", "");
                string name = jsonData.value("name", "");
                string author = jsonData.value("author", "");
                string category = jsonData.value("category", "");
                double price = jsonData.value("price", 0.0);
                int quantity = jsonData.value("quantity", 0);

                if (!isbn.empty() && !name.empty()) {
                    insertBook(isbn, name, author, category, price, quantity);
                }
            }
        }
    }

    // Ejemplo de uso en la búsqueda
    void searchBooks(const vector<string>& searchQueries, const string& outputFileName) {
        ofstream outputFile(outputFileName);

        for (const auto& query : searchQueries) {
            json jsonData;
            try {
                jsonData = json::parse(query);
            }
            catch (json::parse_error& e) {
                cerr << "Error al parsear el JSON: " << e.what() << endl;
                continue;
            }

            string name = jsonData.value("name", "");
            if (!name.empty()) {
                lock_guard<mutex> lock(mtx);
                bool found = false;

                // Iterar sobre el inventario
                for (const auto& pair : inventory) {
                    const string& isbn = pair.first; // Acceder a la clave
                    const BookNode& book = pair.second; // Acceder al valor
                    if (book.name == name) {
                        json j;
                        to_json(j, book); // Convertir a JSON
                        outputFile << j << endl; // Escribir resultado inmediatamente
                        found = true;
                        break; // Encontró el libro, salir del bucle
                    }
                }

                if (!found) {
                    outputFile << json{ {"error", "Book not found"} } << endl; // Mensaje de error si no se encuentra
                }
            }
        }

        outputFile.close();
    }
};

void processSearch(BookInventory& inventory, const vector<string>& searchQueries, const string& outputFileName) {
    inventory.searchBooks(searchQueries, outputFileName);
}

int main() {
    BookInventory inventory;

    // Load operations
    inventory.loadOperations("lab01_books.csv");

    // Read search queries into a vector
    ifstream searchFile("lab01_search.csv");
    string content((istreambuf_iterator<char>(searchFile)), (istreambuf_iterator<char>()));
    searchFile.close();

    stringstream ss(content);
    vector<string> searchQueries;
    string line;

    while (getline(ss, line)) {
        searchQueries.push_back(line);
    }

    // Perform searches in multiple threads
    const int numThreads = 4; // Adjust based on your system's capabilities
    vector<thread> threads;

    for (int i = 0; i < numThreads; ++i) {
        threads.emplace_back(processSearch, ref(inventory), ref(searchQueries), "output.txt");
    }

    for (auto& t : threads) {
        t.join();
    }

    return 0;
}
