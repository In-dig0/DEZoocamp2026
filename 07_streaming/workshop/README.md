# HOMEWORK WEEK7 - STREAM PROCESSING

**Q1: Question 1. Redpanda version**

```sql
docker exec -it 3ceb2979a931 rpk version
```
![Screenshot Q1](./images/Q1.png)

---

**Q2: How long did it take to send the data?**

```sql
uv run producer.py
```
![Screenshot Q2](./images/Q2.png)

---

**Q3: Consumer - trip distance?**

```sql
uv run consumer.py
```
![Screenshot Q3](./images/Q3.png)

---

**Q4: Which PULocationID had the most trips in a single 5-minute window?**

```sql
SELECT window_start, pulocationid, num_trips
FROM processed_trips 
ORDER BY num_trips DESC
LIMIT 1
```
![Screenshot Q4](./images/Q4.png)

---

**Q5: Session window - longest streak?**

```sql
SELECT num_trips
FROM session_events
ORDER BY num_trips DESC
LIMIT 5
```
![Screenshot Q5](./images/Q5.png)

---

**Q6: Session window - longest streak?**

```sql
SELECT window_start, total_tip
FROM tip_stats
ORDER BY total_tip DESC
LIMIT 1
```
![Screenshot Q6](./images/Q6.png)

---