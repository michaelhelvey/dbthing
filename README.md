# dbthing

I wanna explore building a toy rdms. I'm not really sure what I'm doing or how far I'll get but
hopefully I'll learn a thing or two about building databases this way.

## Getting Started

```shell
# builds the database binary into ./zig-out/bin
zig build

# start the database server (in theory, if I write a wire protocol, lol)
./zig-out/bin/db

# ??? some other thing you can use to query the server ???
```

Quite frankly I might not get around to writing the frontend/backend side of this as its by far the
most boring part and I don't think there's necessarily a ton to learn from it. We'll see.

## Goals & Notes

Like I said, I don't really know what I'm doing, but I think my goals look something like this:

**"easy" stuff:**

- [ ] You can write rows to tables. We have a few basic datatypes like text & integers and stuff.
- [ ] You can query tables by doing a seq scan on the table
- [ ] You can create indexes to rows on tables that are stored on disk via a btree
- [ ] You can query tables by the index to get O(1) query perf
- [ ] You can update and delete rows and the indexes get updated with the new pointers

**aaah hard things:**

(basically, ACID)

- [ ] MVCC (multi-version-concurrency-control). I have two processes and one wants to write to the
      table and the other one wants to read from the table. How do I ensure that the writer gets to
      write but the reader sees the _previous_ version until the write is committed? When do I
      update indexes?
- [ ] The regular acid stuff -- atomicity, consistency, isolation (mvcc), and durability. I'll task
      this stuff out when I get to it

## Reading

- [The Design of the Postgres Storage System](https://dsf.berkeley.edu/papers/ERL-M87-06.pdf)
