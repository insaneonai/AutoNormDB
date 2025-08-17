# autoNormDB

**autoNormDB** is a lightweight SQL database engine being developed from scratch as part of my 3-credit lab project. It focuses on building core database components such as schema management, automatic normalization, persistence, and eventually indexing, concurrency control, and full SQL parsing.

---

## 🚧 Project Status

> **Current Focus**: Persistence layer  
> **Next Up**: Concurrency, SQL Parser, and ACID compliance

---

## ✅ Features Implemented

- 🧱 **Schema Management**  
  Define and manage relational schemas.

- 🔁 **Serialization / Deserialization**  
  Save and load schemas, rows, and internal node structures.

- 📦 **Row Storage**  
  Basic row insertion and retrieval.

- Operations Supported (As of Now):
  - Create Table
  - Insert Rows
  - Read all Rows
  - Read with Limit
  - Update by key
  - Update by value
  - Search by key
  - Search by value
  - Read Schema Info

---

## 🔜 Roadmap

- [x] Schema support
- [x] Row and node serialization
- [x] Persistent storage system (in progress)
- [x] B+ Tree indexing (Primary)
- [ ] B+ Tree Partial indexing
- [ ] SQL parser
- [ ] ACID transaction support
- [ ] Concurrency control (locking, isolation levels)
- [ ] HTTP Server
- [ ] JDBC Driver

---


