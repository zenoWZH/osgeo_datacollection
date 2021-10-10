--
-- PostgreSQL database dump
--

-- Dumped from database version 10.17 (Ubuntu 10.17-0ubuntu0.18.04.1)
-- Dumped by pg_dump version 10.17 (Ubuntu 10.17-0ubuntu0.18.04.1)

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
-- Name: plpgsql; Type: EXTENSION; Schema: -; Owner: 
--

CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;


--
-- Name: EXTENSION plpgsql; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION plpgsql IS 'PL/pgSQL procedural language';


SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: aliase; Type: TABLE; Schema: public; Owner: osgeo
--

CREATE TABLE public.aliase (
    aliase_id text NOT NULL,
    mailaddress text NOT NULL,
    person_id text,
    personname text,
    source text
);


ALTER TABLE public.aliase OWNER TO osgeo;

--
-- Name: comment; Type: TABLE; Schema: public; Owner: osgeo
--

CREATE TABLE public.comment (
    comment_id text NOT NULL,
    commit_id text,
    author_aliase_id text NOT NULL,
    "timestamp" timestamp without time zone,
    comment_text text,
    thread_id text
);


ALTER TABLE public.comment OWNER TO osgeo;

--
-- Name: commit; Type: TABLE; Schema: public; Owner: osgeo
--

CREATE TABLE public.commit (
    commit_id text NOT NULL,
    proj_id text NOT NULL,
    commiter_aliase_id text,
    thread_ids text,
    commit_timestamp timestamp without time zone,
    commit_message text,
    author_aliase_id text,
    commit_sha text,
    author_timestamp timestamp without time zone,
    commit_parents text,
    commit_refs text
);


ALTER TABLE public.commit OWNER TO osgeo;

--
-- Name: filelog; Type: TABLE; Schema: public; Owner: osgeo
--

CREATE TABLE public.filelog (
    filelog_id text NOT NULL,
    commit_id text,
    modes text,
    indexes text,
    action text,
    file_name text,
    added text,
    removed text,
    newfile text
);


ALTER TABLE public.filelog OWNER TO osgeo;

--
-- Name: message; Type: TABLE; Schema: public; Owner: osgeo
--

CREATE TABLE public.message (
    message_id text NOT NULL,
    thread_id text,
    author_aliase_id text NOT NULL,
    author_name text NOT NULL,
    receivers_name text,
    message_text text,
    "timestamp" timestamp without time zone
);


ALTER TABLE public.message OWNER TO osgeo;

--
-- Name: people; Type: TABLE; Schema: public; Owner: osgeo
--

CREATE TABLE public.people (
    person_id text NOT NULL,
    name text NOT NULL,
    isdev boolean
);


ALTER TABLE public.people OWNER TO osgeo;

--
-- Name: project; Type: TABLE; Schema: public; Owner: osgeo
--

CREATE TABLE public.project (
    proj_id text NOT NULL,
    proj_name text,
    incubate_status text,
    intro text,
    start_date date,
    end_date date,
    git_repo text,
    sourceforge_repo text,
    website_url text
);


ALTER TABLE public.project OWNER TO osgeo;

--
-- Name: thread; Type: TABLE; Schema: public; Owner: osgeo
--

CREATE TABLE public.thread (
    thread_id text NOT NULL,
    thread_name text NOT NULL,
    project_id text NOT NULL,
    thread_type text,
    thread_status text
);


ALTER TABLE public.thread OWNER TO osgeo;

--
-- Name: comment comment_pkey; Type: CONSTRAINT; Schema: public; Owner: osgeo
--

ALTER TABLE ONLY public.comment
    ADD CONSTRAINT comment_pkey PRIMARY KEY (comment_id);


--
-- Name: commit commit_pkey; Type: CONSTRAINT; Schema: public; Owner: osgeo
--

ALTER TABLE ONLY public.commit
    ADD CONSTRAINT commit_pkey PRIMARY KEY (commit_id);


--
-- Name: aliase emailadd_pkey; Type: CONSTRAINT; Schema: public; Owner: osgeo
--

ALTER TABLE ONLY public.aliase
    ADD CONSTRAINT emailadd_pkey PRIMARY KEY (aliase_id);


--
-- Name: filelog filelog_pkey; Type: CONSTRAINT; Schema: public; Owner: osgeo
--

ALTER TABLE ONLY public.filelog
    ADD CONSTRAINT filelog_pkey PRIMARY KEY (filelog_id);


--
-- Name: message messeage_pkey; Type: CONSTRAINT; Schema: public; Owner: osgeo
--

ALTER TABLE ONLY public.message
    ADD CONSTRAINT messeage_pkey PRIMARY KEY (message_id);


--
-- Name: people people_pkey; Type: CONSTRAINT; Schema: public; Owner: osgeo
--

ALTER TABLE ONLY public.people
    ADD CONSTRAINT people_pkey PRIMARY KEY (person_id);


--
-- Name: project project_pkey; Type: CONSTRAINT; Schema: public; Owner: osgeo
--

ALTER TABLE ONLY public.project
    ADD CONSTRAINT project_pkey PRIMARY KEY (proj_id);


--
-- Name: thread thread_pkey; Type: CONSTRAINT; Schema: public; Owner: osgeo
--

ALTER TABLE ONLY public.thread
    ADD CONSTRAINT thread_pkey PRIMARY KEY (thread_id);


--
-- Name: aliase aliase_person_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: osgeo
--

ALTER TABLE ONLY public.aliase
    ADD CONSTRAINT aliase_person_id_fkey FOREIGN KEY (person_id) REFERENCES public.people(person_id);


--
-- Name: filelog belong_commit; Type: FK CONSTRAINT; Schema: public; Owner: osgeo
--

ALTER TABLE ONLY public.filelog
    ADD CONSTRAINT belong_commit FOREIGN KEY (commit_id) REFERENCES public.commit(commit_id);


--
-- Name: comment comment_author_aliase_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: osgeo
--

ALTER TABLE ONLY public.comment
    ADD CONSTRAINT comment_author_aliase_id_fkey FOREIGN KEY (author_aliase_id) REFERENCES public.aliase(aliase_id);


--
-- Name: commit commit_commiter_aliase_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: osgeo
--

ALTER TABLE ONLY public.commit
    ADD CONSTRAINT commit_commiter_aliase_id_fkey FOREIGN KEY (commiter_aliase_id) REFERENCES public.aliase(aliase_id);


--
-- Name: commit commit_proj_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: osgeo
--

ALTER TABLE ONLY public.commit
    ADD CONSTRAINT commit_proj_id_fkey FOREIGN KEY (proj_id) REFERENCES public.project(proj_id);


--
-- Name: comment issue_thread_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: osgeo
--

ALTER TABLE ONLY public.comment
    ADD CONSTRAINT issue_thread_id_fkey FOREIGN KEY (thread_id) REFERENCES public.thread(thread_id);


--
-- Name: message message_author_aliase_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: osgeo
--

ALTER TABLE ONLY public.message
    ADD CONSTRAINT message_author_aliase_id_fkey FOREIGN KEY (author_aliase_id) REFERENCES public.aliase(aliase_id);


--
-- Name: message message_thread_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: osgeo
--

ALTER TABLE ONLY public.message
    ADD CONSTRAINT message_thread_id_fkey FOREIGN KEY (thread_id) REFERENCES public.thread(thread_id);


--
-- Name: thread thread_project_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: osgeo
--

ALTER TABLE ONLY public.thread
    ADD CONSTRAINT thread_project_id_fkey FOREIGN KEY (project_id) REFERENCES public.project(proj_id);


--
-- PostgreSQL database dump complete
--

