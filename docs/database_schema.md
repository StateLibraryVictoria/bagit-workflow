# Database Schema

## Overview

## Tables

### Collections

A count of collections and how many transfers have been made to it.

#### Columns
- CollectionID (Primary Key)
- Count (Integer)

### Transfers

An item level listing of each transfer as it occurs.

#### Columns

- TransferID (Primary Key, UUID)
- TransferDate
- CollectionID
- Title
- Creator
- PayloadOxum
- ManifestHashMD5