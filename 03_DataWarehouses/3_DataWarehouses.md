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

---

### Recap of dimensional modeling
Third normal form databases are optimized for consistency (rather than speed). For OLTP purposes, this is fine (or even preferable), but for OLAP purposes, we will want to use a de-normalized form, such as a fact & dimension tables.
- **Fact Tables** record business **events**, such as orders, phone calls - stuff that typically symbolizes the relationship between data
- **Dimension Tables** reocrd business **attributes** like individual stores, customers, products, and employees.

However, sometimes it's not always clear what is a *fact* and what is a *dimension*. A simple rule of thumb is **facts are usually numeric and additive**.
For example:
- a review on an article is technically an event, but we can't easily make statistics out of its text content (not a good 'fact')
- invoice numbers might be numeric, but they're not additive (not a good 'fact')
- invoice amount is both numeric and additive (would be a good fact)

Also:
- Date & time are always dimensions
- Physical locations and their attributes are good dimensions candidates
- Products are almost always dimensions

Here's an example from the Udacity course of how you could take a 3NF database and transform it into a de-normalized fact & dimension setup:

![image](media/3nf_to_star.png)
---

### Data warehousing (DWH) architecture

---

### OLAP Cubes

---

### DWH storage technology

