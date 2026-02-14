# DBMS_project
<h2>Project Overview</h2>
<p>
This project explores NoSQL databases with a focus on MongoDB and compares its performance 
with MySQL using the same large-scale sales dataset. 
The study evaluates differences in data modeling, scalability, and system performance 
between relational and non-relational database systems through theoretical analysis 
and practical experimentation.
</p>

<h2>Objectives</h2>
<ul>
  <li>Study NoSQL concepts, architecture, and classifications</li>
  <li>Analyze MongoDB as a document-oriented database system</li>
  <li>Compare MongoDB and MySQL in terms of data model, scalability, and performance</li>
  <li>Evaluate read/write efficiency through large-scale experiments</li>
</ul>

<h2>Database Design</h2>

<h3>Dataset</h3>
<p>
The dataset simulates an e-commerce sales system including customers, products, 
orders, and order details. Data was generated using Python (Faker library).
</p>
<ul>
  <li>~1,000,000 customers</li>
  <li>~10,000,000 orders</li>
  <li>~63,000,000 order details</li>
</ul>

<h3>MySQL (Relational Model)</h3>
<p>
The relational schema consists of four normalized tables:
</p>
<ul>
  <li><code>products</code></li>
  <li><code>consumers</code></li>
  <li><code>orders</code></li>
  <li><code>orderdetails</code></li>
</ul>
<p>
Data integrity is maintained through foreign keys and relational joins.
</p>

<h3>MongoDB (Document Model)</h3>
<p>
Data is stored in a single <code>orders</code> collection. 
Each document embeds customer information and order details 
using nested structures to reduce join operations.
</p>
<ul>
  <li>Embedded customer information</li>
  <li>Nested order details array</li>
  <li>Flexible schema design</li>
</ul>

<h2>Experimental Evaluation</h2>
<p>
Performance was evaluated across four main operations:
</p>
<ul>
  <li>Insert</li>
  <li>Read</li>
  <li>Update</li>
  <li>Delete</li>
</ul>

<p>
Results indicate that MongoDB demonstrates faster write performance 
and better horizontal scalability, while MySQL performs strongly 
in structured relational queries requiring strict consistency.
</p>

<h2>Conclusion</h2>
<p>
MongoDB is well-suited for large-scale, flexible, real-time applications, 
whereas MySQL remains a strong choice for structured systems 
requiring transactional consistency and complex relational operations.
</p>

<h3>Database Structure</h3>
<img width="910" height="568" alt="MySQL Schema" 
src="https://github.com/user-attachments/assets/78e83475-d3ea-4457-b793-169103f1a54d" />
<p><em>Relational database schema in MySQL.</em></p>

<img width="754" height="864" alt="MongoDB Document Structure" 
src="https://github.com/user-attachments/assets/f04ef352-9614-4070-9d5f-9325a75c0e0b" />
<p><em> Document-based data model in MongoDB</em></p>



