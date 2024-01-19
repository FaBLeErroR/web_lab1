import sqlite3
import pandas as pd

con = sqlite3.connect("library.sqlite")
f_damp = open('library.db', 'r', encoding='utf-8-sig')
damp = f_damp.read()
f_damp.close()
con.executescript(damp)

con.commit()
cursor = con.cursor()

cursor.execute('''
select b.book_id, b.title
from book b
where b.book_id in (
    select br.book_id
    from book_reader br)
''')
result = cursor.fetchall()
print(pd.DataFrame(result))
print('\n')


cursor.execute('''
select b.book_id, title, 
    (select genre_name
    from genre g
    where b.genre_id = g.genre_id), 
    (select publisher_name
    from publisher p
    where p.publisher_id = b.publisher_id),
    available_numbers
from book b
where b.book_id not in (
    select br.book_id
    from book_reader br
    where (borrow_date > '2019-12-31') and (borrow_date < '2021-01-01'))
''')
result = cursor.fetchall()
print(pd.DataFrame(result))
print('\n')


df = pd.read_sql('''
select 'I' as Группа, reader_name as Читатель,
    (select count(br.reader_id)
    from  book_reader br
    where br.reader_id = r.reader_id) as Количество
from reader r
where Количество > 6
union
select 'II' as Группа, reader_name as Читатель,
    (select count(br.reader_id)
    from  book_reader br
    where br.reader_id = r.reader_id) as Количество
from reader r
where (Количество >= 3) and (Количество <= 6)
union
select 'III' as Группа, reader_name as Читатель,
    (select count(br.reader_id)
    from  book_reader br
    where br.reader_id = r.reader_id) as Количество
from reader r
where Количество < 3
order by Группа, Читатель, Количество desc
''', con)
print(df)
print('\n')

df = pd.read_sql('''
WITH BookBorrowCounts AS (
    SELECT
        br.book_id,
        b.title AS Название,
        p.publisher_name AS Издательство,
        b.year_publication AS Год,
        COUNT(br.book_reader_id) AS Количество
    FROM
        book_reader br
    JOIN
        book b ON br.book_id = b.book_id
    JOIN
        publisher p ON b.publisher_id = p.publisher_id
    GROUP BY
        br.book_id
),
MaxBorrowCount AS (
    SELECT
        MAX(Количество) AS MaxBorrowCount
    FROM
        BookBorrowCounts
)
SELECT
    Название,
    Издательство,
    Год,
    Количество
FROM
    BookBorrowCounts
WHERE
    Количество = (SELECT MaxBorrowCount FROM MaxBorrowCount)
ORDER BY
    Название ASC,
    Издательство ASC,
    Год DESC;
''', con)
print(df)
print('\n')



cursor.execute('''
       SELECT
           br.book_reader_id,
           br.book_id,
           b.title AS Название,
           br.borrow_date,
           br.return_date
       FROM
           book_reader br
       JOIN
           reader r ON br.reader_id = r.reader_id
       JOIN
           book b ON br.book_id = b.book_id
       WHERE
           r.reader_name = 'Самарин С.С.'
       ORDER BY
           br.borrow_date DESC
       LIMIT 1
   ''')

last_borrowed_book = cursor.fetchone()

if last_borrowed_book:
    book_reader_id, book_id, title, borrow_date, return_date = last_borrowed_book

    cursor.execute('''
           UPDATE book_reader
           SET
               return_date = CURRENT_DATE
           WHERE
               book_reader_id = ?
       ''', (book_reader_id,))

    cursor.execute('''
           UPDATE book
           SET
               available_numbers = available_numbers + 1
           WHERE
               book_id = ?
       ''', (book_id,))

con.commit()

df = pd.read_sql('''
    WITH RankedBooks AS (
        SELECT
            b.title AS Название_книги,
            p.publisher_name AS Название_издательства,
            b.available_numbers AS Количество_экземпляров,
            ROW_NUMBER() OVER (PARTITION BY b.publisher_id ORDER BY b.available_numbers DESC, b.title ASC) AS RowNum
        FROM
            book b
        JOIN
            publisher p ON b.publisher_id = p.publisher_id
        WHERE
            b.available_numbers > 0
    )
    SELECT
        Название_издательства,
        Название_книги,
        Количество_экземпляров
    FROM
        RankedBooks
    WHERE
        RowNum <= 3
    ORDER BY
        Название_издательства ASC,
        Количество_экземпляров DESC,
        Название_книги ASC;
''', con)
print(df)
print('\n')

con.close()