## ZeaOS REPL Design Document
### Overview
ZeaOS is an interactive REPL that orchestrates zeashell libraries (SQL, file I/O, analytics) with Apache Arrow zero-copy tables for fast iterative data analysis. It supports Unix-like pipes via shorthand syntax sugar that compiles to SQL, mounted zeadrive volumes, and plugin execution.
### Command Families
```
text
CORE (tables + assignment)
t1 = load ducks.parquet              # Arrow table from file
t1 = zeaql "SELECT ..."            # Explicit SQL → Arrow table
t1 = t2 | where speed>100            # Shorthand → new table
t1 = t1 | pivot speed→rating         # In-place reassignment

SHORTHAND PIPE SYNTAX (parser → SQL)
| where EXPR                 → SELECT * FROM input WHERE EXPR
| pivot COL→VALUES           → SELECT * FROM input PIVOT COL ON VALUES
| top N                      → SELECT * FROM input LIMIT N
| select COL1,COL2           → SELECT COL1,COL2 FROM input
| group COL [agg(COL)]       → SELECT COL, COUNT(*) FROM input GROUP BY COL

BUILTINS
zeaview t1                     # Render Arrow table
hist                           # Show table lineage DAG
drop t1                        # Remove table from session
zeaplugin fast-ducks           # Run saved pipeline → Arrow table
zeadrive ls /home/data         # Volumez filesystem commands

OS INTEGRATION
ls | zeaql "SELECT COUNT(*) FROM stdin" | zeaview
cat data.parquet | zeaview

```
### Architecture
```
text
zearepl (ZeaOS)
├── readline (input + history)
├── Arrow table registry (session state)
├── Parser (shorthand → SQL)
├── zeashell libs (SQL, file I/O)
├── volumez (zeadrive mount)
├── tview (hist, zeaview)
└── DuckDB CGO (Arrow-native queries)

zeashell and volumez are both existing and in our Open Tempest Labs github  account as separate repos.

zeashell - https://github.com/open-tempest-labs/zeashell
volumez - https://github.com/open-tempest-labs/volumez


```
### Table lifecycle
```
text
ZeaOS> t1 = load ducks.parquet           # Arrow.ReadTable()
ZeaOS> t2 = t1 | where speed>100         # Parser→SQL→new Arrow table
ZeaOS> t1 = t1 | pivot speed→rating      # Reassign (feels mutable)
ZeaOS> hist
t1: ducks.parquet → pivot(speed→rating)  [1M→250K rows]
└─ t2: t1 WHERE speed>100               [250K rows]
ZeaOS> zeaview t1

```
### Zeadrive integration
```
text
ZeaOS> zeadrive mount /home ~           # Volumez mount at session start
ZeaOS> t1 = load ~/zeadrive/ducks.parquet
ZeaOS> zeadrive ls /home/data            # ls, cp, etc. on mounted volume

Entering the repl shell should be done via a command called zeaos.

```
### Startup screen (ASCII art)
```
ZZZZZZZZZZZZZZZZZZZ                                        OOOOOOOOO        SSSSSSSSSSSSSSS 
Z:::::::::::::::::Z                                      OO:::::::::OO    SS:::::::::::::::S
Z:::::::::::::::::Z                                    OO:::::::::::::OO S:::::SSSSSS::::::S
Z:::ZZZZZZZZ:::::Z                                    O:::::::OOO:::::::OS:::::S     SSSSSSS
ZZZZZ     Z:::::Z      eeeeeeeeeeee    aaaaaaaaaaaaa  O::::::O   O::::::OS:::::S            
        Z:::::Z      ee::::::::::::ee  a::::::::::::a O:::::O     O:::::OS:::::S            
       Z:::::Z      e::::::eeeee:::::eeaaaaaaaaa:::::aO:::::O     O:::::O S::::SSSS         
      Z:::::Z      e::::::e     e:::::e         a::::aO:::::O     O:::::O  SS::::::SSSSS    
     Z:::::Z       e:::::::eeeee::::::e  aaaaaaa:::::aO:::::O     O:::::O    SSS::::::::SS  
    Z:::::Z        e:::::::::::::::::e aa::::::::::::aO:::::O     O:::::O       SSSSSS::::S 
   Z:::::Z         e::::::eeeeeeeeeee a::::aaaa::::::aO:::::O     O:::::O            S:::::S
ZZZ:::::Z     ZZZZZe:::::::e         a::::a    a:::::aO::::::O   O::::::O            S:::::S
Z::::::ZZZZZZZZ:::Ze::::::::e        a::::a    a:::::aO:::::::OOO:::::::OSSSSSSS     S:::::S
Z:::::::::::::::::Z e::::::::eeeeeeeea:::::aaaa::::::a OO:::::::::::::OO S::::::SSSSSS:::::S
Z:::::::::::::::::Z  ee:::::::::::::e a::::::::::aa:::a  OO:::::::::OO   S:::::::::::::::SS 
ZZZZZZZZZZZZZZZZZZZ    eeeeeeeeeeeeee  aaaaaaaaaa  aaaa    OOOOOOOOO      SSSSSSSSSSSSSSS 

ZeaOS - Zero-copy Data REPL from Open Tempest Labs

Tables: []  Drive: ~/zeadrive  Plugins: 3
>

We should persist tables in a directory so that we can restart previous sessions from where we left off.
Plugins are discovered by zeashell from the ~/.zea/plugins/ directory and the zeashell command 'zea run --help' can show you the annotation based help content for the plugins in the directory.

Both the zeashell and volumez projects are on this machine in directories of the same names that are siblings to this directory.

```
