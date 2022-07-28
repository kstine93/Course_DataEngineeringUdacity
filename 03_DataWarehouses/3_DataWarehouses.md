# 03-Data Warehouses
_Udacity - Data Engineering Nanodegree_

Syllabus:
1. Basics of Data Warehousing
2. Intro to Amazon Web Services
3. Building a data warehouse on AWS

## Introduction to Data Warehouses

### What is a data warehouse?
#### Business Perspective
- Customers should be able to find goods and make orders
- Staff should be able to stock and re-order goods
- Delivery staff should be able to find and deliver goods
- HR should have an overview of staff
- Management should be able to see sales growth
- etc.

---

**Operational Processes**
- e.g., find goods, make orders, deliver goods

These are typically *online transactional processing* databases (OLTP) and often 3rd normal form

**Analytical Processes**
- e.g., Assess sales staff performance, measure growth

These are typically *online analytical processing* databases (OLAP) and de-normalized

---
Except for very small databases, it's not a good idea to use the SAME data structures for analytical and operational tasks - so we will need to create different (but complementary) structures to support these different needs of the business. **This is where a data warehouse comes into play**

> **A data warehouse is a system which retrieves and consolidates data from source systems (operational processes) for the purpose of enabling analysis**

### Recap of dimensional modeling

### Data warehousing (DWH) architecture

### OLAP Cubes

### DWH storage technology

