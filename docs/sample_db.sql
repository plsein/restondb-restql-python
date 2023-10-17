--
-- PostgreSQL database dump
--

-- Dumped from database version 15.4 (Debian 15.4-1.pgdg120+1)
-- Dumped by pg_dump version 15.3

-- Started on 2023-10-16 23:44:41 UTC

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- TOC entry 3390 (class 1262 OID 16390)
-- Name: sample; Type: DATABASE; Schema: -; Owner: admin
--

CREATE DATABASE "sample" WITH TEMPLATE = template0 ENCODING = 'UTF8' LOCALE_PROVIDER = libc LOCALE = 'en_US.utf8';


ALTER DATABASE "sample" OWNER TO "admin";

\connect "sample"

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- TOC entry 3378 (class 0 OID 16393)
-- Dependencies: 217
-- Data for Name: company; Type: TABLE DATA; Schema: sample; Owner: admin
--

INSERT INTO "sample"."company" VALUES (1, 'The Sparrows');


--
-- TOC entry 3380 (class 0 OID 16401)
-- Dependencies: 219
-- Data for Name: department; Type: TABLE DATA; Schema: sample; Owner: admin
--

INSERT INTO "sample"."department" VALUES (1, 1, 'Finance');
INSERT INTO "sample"."department" VALUES (2, 1, 'Sales');


--
-- TOC entry 3382 (class 0 OID 16411)
-- Dependencies: 221
-- Data for Name: employee; Type: TABLE DATA; Schema: sample; Owner: admin
--

INSERT INTO "sample"."employee" VALUES (1, 1, 'Martin Luther', 'martin.luther@e.mail', 32, 'Accountant', 155000, '2023-01-30 10:10:10', true);
INSERT INTO "sample"."employee" VALUES (2, 1, 'Luke Cage', 'luke.cage@e.mail', 54, 'CFO', 399999, '2021-04-01 10:30:31', true);
INSERT INTO "sample"."employee" VALUES (3, 2, 'Matt Murdock', 'matt.murdock@e.mail', 29, 'Sales Manager', 70000, '2020-07-15 10:30:31', true);
INSERT INTO "sample"."employee" VALUES (7, 2, 'Foggy Nelson', 'foggy.nelson@e.mail', 27, 'Sr. Sales Representative', 40000, '2022-12-17 10:30:31', true);
INSERT INTO "sample"."employee" VALUES (4, 2, 'Karen Page', 'karren.page@e.mail', 21, 'Sales Representative', 10000, '2022-08-01 10:30:31', true);


--
-- TOC entry 3384 (class 0 OID 16419)
-- Dependencies: 223
-- Data for Name: sales; Type: TABLE DATA; Schema: sample; Owner: admin
--

INSERT INTO "sample"."sales" VALUES (1, 'Note11', 50000, 'Steven Pollard', 4, '2023-10-10 10:10:10');
INSERT INTO "sample"."sales" VALUES (2, 'Tab10', 75000, 'Olivia Grreen', 7, '2023-10-11 15:10:31');
INSERT INTO "sample"."sales" VALUES (3, 'Ultrabook Zero1', 190000, 'Barry Allen', 4, '2023-10-11 17:15:51');


--
-- TOC entry 3391 (class 0 OID 0)
-- Dependencies: 220
-- Name: Employee_employee_id_seq; Type: SEQUENCE SET; Schema: sample; Owner: admin
--

SELECT pg_catalog.setval('"sample"."Employee_employee_id_seq"', 7, true);


--
-- TOC entry 3392 (class 0 OID 0)
-- Dependencies: 216
-- Name: company_company_id_seq; Type: SEQUENCE SET; Schema: sample; Owner: admin
--

SELECT pg_catalog.setval('"sample"."company_company_id_seq"', 1, true);


--
-- TOC entry 3393 (class 0 OID 0)
-- Dependencies: 218
-- Name: department_department_id_seq; Type: SEQUENCE SET; Schema: sample; Owner: admin
--

SELECT pg_catalog.setval('"sample"."department_department_id_seq"', 2, true);


--
-- TOC entry 3394 (class 0 OID 0)
-- Dependencies: 222
-- Name: sales_sales_id_seq; Type: SEQUENCE SET; Schema: sample; Owner: admin
--

SELECT pg_catalog.setval('"sample"."sales_sales_id_seq"', 11, true);


-- Completed on 2023-10-16 23:44:41 UTC

--
-- PostgreSQL database dump complete
--

