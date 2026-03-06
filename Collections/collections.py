import sys
from Collections.restoreCollections import restoreCollections
from Collections.exportCollections import exportCollections

def collections():
    while True:
        print("""
        1. Create Collection
        2. Update Collection
        3. List Collections
        4. Delete Collection
        5. Export Collections
        6. Restore Collections (using the exported file)
        7. Go to Data Map
        8. Go to Purview 
        9. Exit
        """)
        choice = input("Enter your choice: ")
        if choice == "1":
            createCollection()
        elif choice == "2":
            updateCollection()
        elif choice == "3":
            listCollections()
        elif choice == "4":
            deleteCollection()
        elif choice == "5":
            exportCollections()
        elif choice == "6":
            restoreCollections()
        elif choice == "7":
            from DataMap.dataMap import datamap
            dataMap.datamap()
        elif choice == "8":
                import purview# Import inside the function
                purview.purview()
        elif choice == "9":
            print("Exiting... Goodbye!")
            sys.exit(0)
        else:
            print("Incorrect choice, try again.\n")
    
if __name__ == "__main__":
    collections()