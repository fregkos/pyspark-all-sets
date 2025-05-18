# Matching Bounds for the All-Pairs Map-Reduce Problem

## Introduction and problem setup

- Fixed $d = 5000$, $q = 1000$ where:
  - d = original data elements
  - q = maximum capacity of elements per reducer
- While the choice of these specific numbers allows for the optimal solution to be achieved, the proposed method still works optimally for other values of `d` and/or `q`, even if it results in slightly unequally sized groups; without loss of generality.
- We continue with the aforementioned values as they allow for easier examination of the consequences each partitioning approach has.

## Goal

Find the smallest replication rate $r$ in order to minimize network traffic while maintaining parallelism.

## Selecting the Number of Groups

### Naive approaches

We know the maximum capacity of each reducer is $1000$ elements. Therefore, each group can have at most $\frac{1000}{2}=500$ elements, so that at any point two groups can fit in a reducer, so they can be compared with one another. Hence, we assess the following configurations:

- Each reducer requires the $\frac{d}{g}$ members of two different groups to compare, thus $q=2\times\frac{d}{g} \Leftrightarrow g=2\times\frac{d}{q}$
- Let us choose to use a reducer size of  $q=1000$.
  - $g=2\times\frac{d}{q} \Rightarrow g = 2\times\frac{5000}{1000} = 10$ **pairs** of groups
  - Therefore, $\frac{d}{g}=\frac{5000}{10}=500$ items per group
  - $2\times500 = 1000$ elements per `reducer` $\equiv q = 1000$
  - This would be an *edge case* scenario, as **no more than** 1000 elements could fit in a reducer at once in any given moment. The reducers would be maximally utilized.

- Let us choose to use a reducer size of $q/2=500$.
  - $g=2\times\frac{d}{q} \Rightarrow g = 2\times\frac{5000}{500} = 20$ **pairs** of groups
  - Therefore, $\frac{d}{g}=\frac{5000}{20}=250$ items per group
  - $2\times250 = 500$ elements per `reducer` $\equiv q/2 = 500 < q=1000 $
  - This would be a *feasible* scenario and one of under-utilization, as 500 elements could fit easily in a reducer at once in any given moment.

- Let us choose to use a reducer size of $q/10=100$.
  - $g=2\times\frac{d}{q} \Rightarrow g = 2\times\frac{5000}{100} = 100$ **pairs** of groups
  - Therefore, $\frac{d}{g}=\frac{5000}{100}=50$ items per group
  - $2\times50 = 100$ elements per `reducer` $\equiv q/10 = 100 \ll q = 1000$
  - This would be a *feasible*, but quite an under-utilized scenario, as 200 elements could fit in a reducer at once in any given moment and utilize only $\frac{100}{1000}=10\%$ of the reducer's capacity. We would need $9$ more groups to utilize full capacity.

<!-- TODO: Recheck the utilization explanation. Note that below this line, corrections are required. Above that, perhaps no. -->

- $g=2\times\frac{d}{q} \Rightarrow g = 2\times\frac{5000}{100} = 100$ groups, with $100$ elements each (_20% reducer capacity_)
  - Thus the reducer can have 10 groups at any point in time to output combinations of pairs

  The above grouping methods would result in the following replication rates (respectively):

  - $r = 2\times \frac{d}{q}-1=2\times\frac{5000}{1000}-1=10-1=9$
  - $r = 2\times \frac{d}{q}-1=2\times\frac{5000}{500}-1=20-1=19$
  - $r = 2\times \frac{d}{q}-1=2\times\frac{5000}{100}-1=100-1=99$

However, we know the **best** possible lower bound approximates $r=\lceil\frac{d-1}{q-1}\rceil=\lceil\frac{5000-1}{1000-1}\rceil=6$

So this begs the question, what is the optimal number of groups, so that the replication rate is minimized?

#### Best possible lower bound

$r = \frac{d}{q} + 1$

---

### Proposed approach

Find a prime $p$ which satisfies:

1. $p^2$ divides $d$
2. $q=\frac{d}{p}$
3. $group\_size = \frac{d}{p^2}$

Let us consider the following example. If that prime is $p=5$, our data $d$ is $5000$ in total and the maximum capacity of each reducer $q$ is $1000$, then:

1. $groups = p^2 \Rightarrow 5^2 = 25 \newline 5^2 | 5000 \Rightarrow 5000 \bmod 25 = 0 \Rightarrow True$

2. $q=\frac{d}{p} \Rightarrow q = \frac{5000}{5} = 1000$, _which respects maximum capacity requirement / fixed rule_
3. $group\_size = \frac{d}{p^2} \Rightarrow \frac{5000}{25} = 200$

Therefore, each groups' size must be $200$ elements, meaning each reducer receives $\frac{q}{group\_size} = \frac{1000}{200}=5$ groups per reducer

#### Lower bounds

Based on the paper's findings (Theorem 3.1), $r = \frac{d}{q} + 1$ since the above conditions are satisfied.

Ergo, the replication rate becomes: $r=\frac{d}{q} + 1=\frac{5000}{1000} + 1=6$, Q.E.D. (quod erat demonstrandum, όπερ έδει δείξαι).
