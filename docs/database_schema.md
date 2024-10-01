# Database Schema

## Overview

## Tables

### Collections

A count of collections and how many transfers have been made to it.

#### Columns
- InternalSenderID (Primary Key)
- Count (Integer)

### Transfers

An item level listing of each transfer as it occurs.

#### Columns

- TransferID (INTEGER PRIMARY KEY AUTOINCREMENT)
- InternalSenderID
- BagUUID
- TransferDate
- PayloadOxum
- ManifestSHA256Hash
- TransferTimeSeconds