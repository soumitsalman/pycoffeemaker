# RSS repeated-content scan

Scanned **13,849 configured RSS URLs** from `factory/feeds.yaml` using `RSSFeedCollector` and the collector's `is_bean_storable` filter.

Similarity is Jaccard similarity over case-folded, unique word tokens in each item's `content`: intersection size divided by union size. Matches require a score **greater than 90%**. Connected match groups can include an item linked through multiple qualifying pairs; scores below are for the qualifying pair edges.

- Feeds returning entries: 11,492
- Feed entries returned: 532,686
- Storable entries checked: 143,876
- Fetch/parse exceptions: 402
- Matching groups: 326
- Runtime: 42.8 minutes

## Matches

### 1. 20 items; pair scores 100.0%–100.0% (mean 100.0%, 190 qualifying pairs)

Feed: <https://footballgroundguide.com/feed>

- [Hill Dickinson Stadium Tour Guide: Tickets, Prices, What’s Included and FAQs](https://footballgroundguide.com/news/everton-hill-dickinson-stadium-tour-guide.html)
- [How to get to Poznan: Best ways for Sunderland fans to travel for UEFA Europa League clash](https://footballgroundguide.com/news/how-to-get-to-poznan-best-ways-for-sunderland-fans-to-travel-for-uefa-europa-league-clash.html)
- [Villa Park Stadium Tour Guide: Tickets, Prices, What’s Included and FAQs](https://footballgroundguide.com/news/aston-villa-villa-park-stadium-tour-guide.html)
- … and 17 more entries

### 2. 16 items; pair scores 90.1%–99.3% (mean 95.5%, 109 qualifying pairs)

Feed: <https://quarkus.io/feed.xml>

- [Quarkus 3.8.6 released - Maintenance release](https://quarkus.io/blog/quarkus-3-8-6-released)
- [Quarkus 3.11.3 released - Maintenance release](https://quarkus.io/blog/quarkus-3-11-3-released)
- [Quarkus 3.11.2 released - Maintenance release](https://quarkus.io/blog/quarkus-3-11-2-released)
- … and 13 more entries

### 3. 10 items; pair scores 90.1%–94.2% (mean 92.4%, 10 qualifying pairs)

Feed: <https://feeds.simplecast.com/dLRotFGk>

- [Make Your Problems Smaller](http://www.developertea.com)
- [3 Principles for Your Job Search](http://www.developertea.com)
- [Contingencies and Planning for Failure](http://www.developertea.com)
- … and 7 more entries

### 4. 9 items; pair scores 97.9%–100.0% (mean 99.2%, 36 qualifying pairs)

Feed: <https://etcd.io/index.xml>

- [Benchmarking etcd v2.2.0-rc-memory](https://etcd.io/docs/v3.4/benchmarks/etcd-2-2-0-rc-memory-benchmarks)
- [Benchmarking etcd v2.2.0-rc-memory](https://etcd.io/docs/v3.5/benchmarks/etcd-2-2-0-rc-memory-benchmarks)
- [Benchmarking etcd v2.2.0-rc-memory](https://etcd.io/docs/v3.6/benchmarks/etcd-2-2-0-rc-memory-benchmarks)
- … and 6 more entries

### 5. 9 items; pair scores 96.9%–100.0% (mean 98.7%, 36 qualifying pairs)

Feed: <https://etcd.io/index.xml>

- [Benchmarking etcd v2.1.0](https://etcd.io/docs/v3.4/benchmarks/etcd-2-1-0-alpha-benchmarks)
- [Benchmarking etcd v2.1.0](https://etcd.io/docs/v3.5/benchmarks/etcd-2-1-0-alpha-benchmarks)
- [Benchmarking etcd v2.1.0](https://etcd.io/docs/v3.6/benchmarks/etcd-2-1-0-alpha-benchmarks)
- … and 6 more entries

### 6. 9 items; pair scores 97.7%–98.5% (mean 98.3%, 36 qualifying pairs)

Feed: <https://secret-archive.org/feed/>

- [Fusce nec morbi](https://secret-archive.org/fusce-nec-morbi?utm_source=rss&utm_medium=rss&utm_campaign=fusce-nec-morbi)
- [Dolore gravida](https://secret-archive.org/dolore-gravida?utm_source=rss&utm_medium=rss&utm_campaign=dolore-gravida)
- [Quisque integer](https://secret-archive.org/quisque-integer?utm_source=rss&utm_medium=rss&utm_campaign=quisque-integer)
- … and 6 more entries

### 7. 8 items; pair scores 100.0%–100.0% (mean 100.0%, 28 qualifying pairs)

Feed: <https://cozystack.io/index.xml>

- [Virtual Machine Resources](https://cozystack.io/docs/v0/virtualization/resources)
- [Virtual Machine Resources](https://cozystack.io/docs/v1.0/virtualization/resources)
- [Virtual Machine Resources](https://cozystack.io/docs/v1.1/virtualization/resources)
- … and 5 more entries

### 8. 8 items; pair scores 91.2%–97.0% (mean 93.8%, 23 qualifying pairs)

Feed: <https://www.lucioarese.net/feed/>

- [Tips On Managing Business](https://www.lucioarese.net/2016/03/08/tips-on-managing-business)
- [Tech Conference 2016](https://www.lucioarese.net/2016/03/08/tech-conference-2016)
- [Big Data Startup](https://www.lucioarese.net/2016/03/08/big-data-startup)
- … and 5 more entries

### 9. 8 items; pair scores 90.1%–94.1% (mean 91.6%, 19 qualifying pairs)

Feed: <https://crossborderrail.eu/feed/>

- [Live Blog – #CrossBorderRail #LongestTrainEU Day 13 – 21 September – Bruxelles – Berlin](https://crossborderrail.eu/live-blog-21-sep-2026)
- [Live Blog – #CrossBorderRail #LongestTrainEU Day 12 – 18 September – Ravières – Paris – Bruxelles](https://crossborderrail.eu/live-blog-18-sep-2026)
- [Live Blog – #CrossBorderRail #LongestTrainEU Day 11 – 17 September – Ax les Thermes – Toulouse – Bordeaux – Paris – Ravières](https://crossborderrail.eu/live-blog-17-sep-2026)
- … and 5 more entries

### 10. 7 items; pair scores 100.0%–100.0% (mean 100.0%, 21 qualifying pairs)

Feed: <https://cozystack.io/index.xml>

- [Monitoring Parameters](https://cozystack.io/docs/v0/operations/services/monitoring/parameters)
- [Monitoring Parameters](https://cozystack.io/docs/v1.0/operations/services/monitoring/parameters)
- [Monitoring Parameters](https://cozystack.io/docs/v1.1/operations/services/monitoring/parameters)
- … and 4 more entries

### 11. 7 items; pair scores 100.0%–100.0% (mean 100.0%, 21 qualifying pairs)

Feed: <https://cozystack.io/index.xml>

- [Network Architecture](https://cozystack.io/docs/v1.0/networking/architecture)
- [Network Architecture](https://cozystack.io/docs/v1.1/networking/architecture)
- [Network Architecture](https://cozystack.io/docs/v1.2/networking/architecture)
- … and 4 more entries

### 12. 7 items; pair scores 100.0%–100.0% (mean 100.0%, 21 qualifying pairs)

Feed: <https://tinygo.org/index.xml>

- [circuitplay-express](https://tinygo.org/docs/reference/microcontrollers/machine/circuitplay-express)
- [feather-m0](https://tinygo.org/docs/reference/microcontrollers/machine/feather-m0)
- [itsybitsy-m0](https://tinygo.org/docs/reference/microcontrollers/machine/itsybitsy-m0)
- … and 4 more entries

### 13. 7 items; pair scores 99.2%–100.0% (mean 99.5%, 21 qualifying pairs)

Feed: <https://cozystack.io/index.xml>

- [SeaweedFS Service Reference](https://cozystack.io/docs/v1.0/operations/services/seaweedfs)
- [SeaweedFS Service Reference](https://cozystack.io/docs/v1.1/operations/services/seaweedfs)
- [SeaweedFS Service Reference](https://cozystack.io/docs/v1.2/operations/services/seaweedfs)
- … and 4 more entries

### 14. 7 items; pair scores 98.1%–100.0% (mean 99.4%, 21 qualifying pairs)

Feed: <https://cozystack.io/index.xml>

- [Cluster Autoscaler for Azure](https://cozystack.io/docs/v1.0/operations/multi-location/autoscaling/azure)
- [Cluster Autoscaler for Azure](https://cozystack.io/docs/v1.1/operations/multi-location/autoscaling/azure)
- [Cluster Autoscaler for Azure](https://cozystack.io/docs/v1.2/operations/multi-location/autoscaling/azure)
- … and 4 more entries

### 15. 7 items; pair scores 92.0%–100.0% (mean 96.3%, 21 qualifying pairs)

Feed: <https://etcd.io/index.xml>

- [API reference: concurrency](https://etcd.io/docs/v3.4/dev-guide/api_concurrency_reference_v3)
- [API reference: concurrency](https://etcd.io/docs/v3.5/dev-guide/api_concurrency_reference_v3)
- [API reference: concurrency](https://etcd.io/docs/v3.6/dev-guide/api_concurrency_reference_v3)
- … and 4 more entries

### 16. 6 items; pair scores 99.0%–100.0% (mean 99.7%, 15 qualifying pairs)

Feed: <https://cozystack.io/index.xml>

- [Managed NATS Service](https://cozystack.io/docs/v0/applications/nats)
- [Managed NATS Service](https://cozystack.io/docs/v1.0/applications/nats)
- [Managed NATS Service](https://cozystack.io/docs/v1.1/applications/nats)
- … and 3 more entries

### 17. 6 items; pair scores 98.7%–99.2% (mean 99.0%, 15 qualifying pairs)

Feed: <https://discuss.huggingface.co/posts.rss>

- [+1-(833)-457-9024 | Vueling Airlines Wilmington Office, NC 🇺🇸](https://discuss.huggingface.co/t/1-833-457-9024-vueling-airlines-wilmington-office-nc/182755#post_1)
- [+1-(833)-457-9024 | Latam Airlines Wilmington Office, NC 🇺🇸](https://discuss.huggingface.co/t/1-833-457-9024-latam-airlines-wilmington-office-nc/182754#post_1)
- [+1-(833)-457-9024 | Porter Airlines Wilmington Office, NC 🇺🇸](https://discuss.huggingface.co/t/1-833-457-9024-porter-airlines-wilmington-office-nc/182753#post_1)
- … and 3 more entries

### 18. 6 items; pair scores 98.9%–98.9% (mean 98.9%, 15 qualifying pairs)

Feed: <https://discuss.huggingface.co/posts.rss>

- [🌟【+1✦877✺370✧8278】Is Japan Airlines Customer Care Available All Day? Learn How to Reach a Representative for Reservations, Changes & Refund Questions↝Get Help(‘Complete FAQs’)](https://discuss.huggingface.co/t/1-877-370-8278-is-japan-airlines-customer-care-available-all-day-learn-how-to-reach-a-representative-for-reservations-changes-refund-questions-get-help-complete-faqs/182748#post_1)
- [✈️【+1✧877★370❖8278】Is Contour Airlines Open Day and Night for Customer Support? Learn How to Get Help With Bookings, Changes & Travel Issues↝Support Guide(‘FAQs’)](https://discuss.huggingface.co/t/1-877-370-8278-is-contour-airlines-open-day-and-night-for-customer-support-learn-how-to-get-help-with-bookings-changes-travel-issues-support-guide-faqs/182747#post_1)
- [🌺【+1✦877✤370✥8278】How Do I Reach Frontier Customer Service Outside Regular Hours? Explore Live Agent Assistance, Online Support & Travel Help↝Complete Guide(‘Expert FAQs’)](https://discuss.huggingface.co/t/1-877-370-8278-how-do-i-reach-frontier-customer-service-outside-regular-hours-explore-live-agent-assistance-online-support-travel-help-complete-guide-expert-faqs/182743#post_1)
- … and 3 more entries

### 19. 6 items; pair scores 95.9%–100.0% (mean 98.6%, 15 qualifying pairs)

Feed: <https://cozystack.io/index.xml>

- [Managed Kafka Service](https://cozystack.io/docs/v0/applications/kafka)
- [Managed Kafka Service](https://cozystack.io/docs/v1.0/applications/kafka)
- [Managed Kafka Service](https://cozystack.io/docs/v1.1/applications/kafka)
- … and 3 more entries

### 20. 6 items; pair scores 95.2%–100.0% (mean 97.0%, 15 qualifying pairs)

Feed: <https://cynicaldeveloper.com/feed/podcast>

- [Episode 103 - Machine Learning and Artificial Intelligence - Part 3](http://cynicaldeveloper.com/podcast/103)
- [Episode 102 - Machine Learning and Artificial Intelligence - Part 2](http://cynicaldeveloper.com/podcast/102)
- [Episode 101 - Machine Learning and Artificial Intelligence - Part 1](http://cynicaldeveloper.com/podcast/101)
- … and 3 more entries

### 21. 6 items; pair scores 90.8%–100.0% (mean 95.1%, 9 qualifying pairs)

Feed: <https://etcd.io/index.xml>

- [API reference](https://etcd.io/docs/v3.4/dev-guide/api_reference_v3)
- [API reference](https://etcd.io/docs/v3.5/dev-guide/api_reference_v3)
- [API reference](https://etcd.io/docs/v3.6/dev-guide/api_reference_v3)
- … and 3 more entries

### 22. 6 items; pair scores 90.4%–97.8% (mean 94.6%, 10 qualifying pairs)

Feed: <https://www.sdr-radio.com/feed/rss2>

- [SDR Television v1.1.6](https://www.sdr-radio.com/sdr-television-v1-1-6)
- [SDR Television v1.1.4](https://www.sdr-radio.com/sdr-television-v1-1-4)
- [SDR Television v1.1.3](https://www.sdr-radio.com/sdr-television-v1-1-3)
- … and 3 more entries

### 23. 6 items; pair scores 90.0%–96.7% (mean 93.2%, 10 qualifying pairs)

Feed: <https://rss.art19.com/tim-ferriss-show>

- [Ep 51: Tim Answers 10 More Popular Questions from Listeners](https://rss.art19.com/episodes/7a49c2db-e557-493c-871e-f00277cd4a7a.mp3?rss_browser=bahjigtdahjvbwugogzfva%3d%3d--d05363d83ce333c74f32188013892b2863ad051c)
- [Ep 49: Tim Answers Your 10 Most Popular Questions](https://rss.art19.com/episodes/666e1714-634a-4e98-8f91-ef56994b562f.mp3?rss_browser=bahjigtdahjvbwugogzfva%3d%3d--d05363d83ce333c74f32188013892b2863ad051c)
- [Ep 44: How to Avoid Decision Fatigue (<20 Min)](https://rss.art19.com/episodes/d92071f7-abc7-49f8-99c2-7651a75d9319.mp3?rss_browser=bahjigtdahjvbwugogzfva%3d%3d--d05363d83ce333c74f32188013892b2863ad051c)
- … and 3 more entries

### 24. 5 items; pair scores 100.0%–100.0% (mean 100.0%, 10 qualifying pairs)

Feed: <https://cozystack.io/index.xml>

- [Managed ClickHouse Service](https://cozystack.io/docs/v0/applications/clickhouse)
- [Managed ClickHouse Service](https://cozystack.io/docs/v1.0/applications/clickhouse)
- [Managed ClickHouse Service](https://cozystack.io/docs/v1.1/applications/clickhouse)
- … and 2 more entries

### 25. 5 items; pair scores 98.9%–100.0% (mean 99.6%, 10 qualifying pairs)

Feed: <https://cozystack.io/index.xml>

- [Managed Harbor Container Registry](https://cozystack.io/docs/v1.0/applications/harbor)
- [Managed Harbor Container Registry](https://cozystack.io/docs/v1.1/applications/harbor)
- [Managed Harbor Container Registry](https://cozystack.io/docs/v1.2/applications/harbor)
- … and 2 more entries

### 26. 5 items; pair scores 98.9%–100.0% (mean 99.6%, 10 qualifying pairs)

Feed: <https://cozystack.io/index.xml>

- [Managed OpenBAO Service](https://cozystack.io/docs/v1.0/applications/openbao)
- [Managed OpenBAO Service](https://cozystack.io/docs/v1.1/applications/openbao)
- [Managed OpenBAO Service](https://cozystack.io/docs/v1.2/applications/openbao)
- … and 2 more entries

### 27. 5 items; pair scores 98.0%–99.7% (mean 98.7%, 10 qualifying pairs)

Feed: <https://redocly.com/docs/changelog/feed.xml>

- [Realm 0.137.0](https://redocly.com/docs/realm/changelog#realm%400.137.0)
- [Reef 0.137.0](https://redocly.com/docs/realm/changelog#reef%400.137.0)
- [Revel 0.137.0](https://redocly.com/docs/realm/changelog#revel%400.137.0)
- … and 2 more entries

### 28. 5 items; pair scores 90.3%–96.5% (mean 94.1%, 5 qualifying pairs)

Feed: <https://www.gathering4gardner.org/feed/>

- [Friday News Sep 25: Weekly Socials, Virtual CoM, YT Videos](https://www.gathering4gardner.org/news-2026-09-25)
- [Friday News Sep 18: Weekly Socials, September Virtual CoM, YT Videos](https://www.gathering4gardner.org/news-2026-09-18)
- [Friday News Sep 11: Weekly Socials, Virtual CoM, YT Videos](https://www.gathering4gardner.org/news-2026-09-11)
- … and 2 more entries

### 29. 4 items; pair scores 100.0%–100.0% (mean 100.0%, 6 qualifying pairs)

Feed: <https://cozystack.io/index.xml>

- [FoundationDB](https://cozystack.io/docs/v1.0/applications/foundationdb)
- [FoundationDB](https://cozystack.io/docs/v1.1/applications/foundationdb)
- [FoundationDB](https://cozystack.io/docs/v1.2/applications/foundationdb)
- … and 1 more entries

### 30. 4 items; pair scores 100.0%–100.0% (mean 100.0%, 6 qualifying pairs)

Feed: <https://oscarstories.com/rss.xml>

- [Alice's Abenteuer im Wunderland - Hinunter in den Kaninchenbau](https://oscarstories.com/blog/de/alice-in-wonderland-chapter-12-de)
- [Alice's Abenteuer im Wunderland - Hinunter in den Kaninchenbau](https://oscarstories.com/blog/de/alice-in-wonderland-chapter-1-de)
- [Alice's Abenteuer im Wunderland - Hinunter in den Kaninchenbau](https://oscarstories.com/blog/de/alice-in-wonderland-chapter-12)
- … and 1 more entries

### 31. 4 items; pair scores 100.0%–100.0% (mean 100.0%, 6 qualifying pairs)

Feed: <https://www.hyundai.com/wsvc/ww/rss/newsroom.newsroom.do>

- [(Video) Hyundai Motor Group Presents Its Vision to Popularize Hydrogen by 2040 at Hydrogen Wave Forum (3)](https://www.hyundai.com/worldwide/en/newsroom/detail/0000000469)
- [(Video) Hyundai Motor Group Presents Its Vision to Popularize Hydrogen by 2040 at Hydrogen Wave Forum (2)](https://www.hyundai.com/worldwide/en/newsroom/detail/0000000470)
- [(Video) Hyundai Motor Group Presents Its Vision to Popularize Hydrogen by 2040 at Hydrogen Wave Forum (1)](https://www.hyundai.com/worldwide/en/newsroom/detail/0000000471)
- … and 1 more entries

### 32. 4 items; pair scores 99.3%–100.0% (mean 99.7%, 6 qualifying pairs)

Feed: <https://cozystack.io/index.xml>

- [Platform Package Reference](https://cozystack.io/docs/v1.0/operations/configuration/platform-package)
- [Platform Package Reference](https://cozystack.io/docs/v1.1/operations/configuration/platform-package)
- [Platform Package Reference](https://cozystack.io/docs/v1.2/operations/configuration/platform-package)
- … and 1 more entries

### 33. 4 items; pair scores 95.7%–100.0% (mean 97.1%, 6 qualifying pairs)

Feed: <https://etcd.io/index.xml>

- [Libraries and tools](https://etcd.io/docs/v3.5/integrations)
- [Libraries and tools](https://etcd.io/docs/v3.6/integrations)
- [Libraries and tools](https://etcd.io/docs/v3.7/integrations)
- … and 1 more entries

### 34. 4 items; pair scores 92.2%–100.0% (mean 96.6%, 4 qualifying pairs)

Feed: <https://www.indy100.com/feeds/feed.rss>

- ['Mar-a-Lago Face' is a problematic talking point - no matter which side of politics you're on](https://www.indy100.com/politics/trump/maralago-face-makeup-trend-republican-women-us-politics-2674407720)
- ['Mar-a-Lago Face' is a problematic trend - no matter which political party you support](https://www.indy100.com/politics/trump/maralago-face-makeup-trend-republican-women-us-politics-2674407720)
- ['Mar-a-Lago Face' is a problematic trend - no matter which political party you support](https://www.indy100.com/politics/trump/maralago-face-makeup-trend-republican-women-us-politics-2674407720)
- … and 1 more entries

### 35. 4 items; pair scores 94.6%–97.8% (mean 96.2%, 6 qualifying pairs)

Feed: <https://catonmat.net/feed>

- [6.65 Million Google Clicks! 💸](https://catonmat.net/665-million-google-clicks)
- [6.64 Million Google Clicks! 💸](https://catonmat.net/664-million-google-clicks)
- [6.63 Million Google Clicks! 💸](https://catonmat.net/663-million-google-clicks)
- … and 1 more entries

### 36. 3 items; pair scores 100.0%–100.0% (mean 100.0%, 3 qualifying pairs)

Feed: <https://cozystack.io/index.xml>

- [FoundationDB](https://cozystack.io/docs/v1.4/applications/foundationdb)
- [FoundationDB](https://cozystack.io/docs/v1.5/applications/foundationdb)
- [FoundationDB](https://cozystack.io/docs/v1.6/applications/foundationdb)

### 37. 3 items; pair scores 100.0%–100.0% (mean 100.0%, 3 qualifying pairs)

Feed: <https://open-web-advocacy.org/feed.xml>

- [Open Letter: Why the CMA Must Enforce the DMCCA](https://open-web-advocacy.org/blog/open-letter--why-the-cma-must-enforce-the-dmcca)
- [Open Letter: Why the CMA Must Enforce the DMCCA](https://open-web-advocacy.org/ja/blog/open-letter--why-the-cma-must-enforce-the-dmcca)
- [Open Letter: Why the CMA Must Enforce the DMCCA](https://open-web-advocacy.org/es/blog/open-letter--why-the-cma-must-enforce-the-dmcca)

### 38. 3 items; pair scores 100.0%–100.0% (mean 100.0%, 3 qualifying pairs)

Feed: <https://open-web-advocacy.org/feed.xml>

- [OWA on RedMonk: Why the Mobile Web Still Can’t Compete with Native Apps, and How to Fix It!](https://open-web-advocacy.org/blog/owa-on-redmonk--why-the-mobile-web-still-cant-compete-with-native-apps-and-how-to-fix-it)
- [OWA on RedMonk: Why the Mobile Web Still Can’t Compete with Native Apps, and How to Fix It!](https://open-web-advocacy.org/ja/blog/owa-on-redmonk--why-the-mobile-web-still-cant-compete-with-native-apps-and-how-to-fix-it)
- [OWA on RedMonk: Why the Mobile Web Still Can’t Compete with Native Apps, and How to Fix It!](https://open-web-advocacy.org/es/blog/owa-on-redmonk--why-the-mobile-web-still-cant-compete-with-native-apps-and-how-to-fix-it)

### 39. 3 items; pair scores 100.0%–100.0% (mean 100.0%, 3 qualifying pairs)

Feed: <https://open-web-advocacy.org/feed.xml>

- [Apple’s Interoperability Commitments to the UK’s CMA Promise Nothing](https://open-web-advocacy.org/blog/apples-interoperability-commitments-to-the-uk-cma-promise-nothing)
- [Apple’s Interoperability Commitments to the UK’s CMA Promise Nothing](https://open-web-advocacy.org/ja/blog/apples-interoperability-commitments-to-the-uk-cma-promise-nothing)
- [Apple’s Interoperability Commitments to the UK’s CMA Promise Nothing](https://open-web-advocacy.org/es/blog/apples-interoperability-commitments-to-the-uk-cma-promise-nothing)

### 40. 3 items; pair scores 100.0%–100.0% (mean 100.0%, 3 qualifying pairs)

Feed: <https://open-web-advocacy.org/feed.xml>

- [What Apple’s UK Strategic Market Status Designation means for Browsers and Web Apps](https://open-web-advocacy.org/blog/what-apples-uk-strategic-market-status-designation-means-for-browsers-and-web-apps)
- [What Apple’s UK Strategic Market Status Designation means for Browsers and Web Apps](https://open-web-advocacy.org/ja/blog/what-apples-uk-strategic-market-status-designation-means-for-browsers-and-web-apps)
- [What Apple’s UK Strategic Market Status Designation means for Browsers and Web Apps](https://open-web-advocacy.org/es/blog/what-apples-uk-strategic-market-status-designation-means-for-browsers-and-web-apps)

### 41. 3 items; pair scores 100.0%–100.0% (mean 100.0%, 3 qualifying pairs)

Feed: <https://open-web-advocacy.org/feed.xml>

- [Japan: Apple Must Lift Browser Engine Ban by December](https://open-web-advocacy.org/blog/japan-apple-must-lift-engine-ban-by-december)
- [Japan: Apple Must Lift Engine Ban by December](https://open-web-advocacy.org/ja/blog/japan-apple-must-lift-engine-ban-by-december)
- [Japan: Apple Must Lift Engine Ban by December](https://open-web-advocacy.org/es/blog/japan-apple-must-lift-engine-ban-by-december)

### 42. 3 items; pair scores 100.0%–100.0% (mean 100.0%, 3 qualifying pairs)

Feed: <https://open-web-advocacy.org/feed.xml>

- [UK Regulator Flags Apple’s iOS Browser Engine Ban in Draft SMS Designation](https://open-web-advocacy.org/blog/uk-regulator-flags-apples-ios-browser-engine-ban-in-draft-sms-designation)
- [UK Regulator Flags Apple’s iOS Browser Engine Ban in Draft SMS Designation](https://open-web-advocacy.org/ja/blog/uk-regulator-flags-apples-ios-browser-engine-ban-in-draft-sms-designation)
- [UK Regulator Flags Apple’s iOS Browser Engine Ban in Draft SMS Designation](https://open-web-advocacy.org/es/blog/uk-regulator-flags-apples-ios-browser-engine-ban-in-draft-sms-designation)

### 43. 3 items; pair scores 100.0%–100.0% (mean 100.0%, 3 qualifying pairs)

Feed: <https://open-web-advocacy.org/feed.xml>

- [Balancing Security and Fair Competition](https://open-web-advocacy.org/blog/balancing-security-and-fair-competition)
- [Balancing Security and Fair Competition](https://open-web-advocacy.org/ja/blog/balancing-security-and-fair-competition)
- [Balancing Security and Fair Competition](https://open-web-advocacy.org/es/blog/balancing-security-and-fair-competition)

### 44. 3 items; pair scores 100.0%–100.0% (mean 100.0%, 3 qualifying pairs)

Feed: <https://open-web-advocacy.org/feed.xml>

- [Industry Voices Caution Against DOJ’s Plan to Force Sale Of Chrome](https://open-web-advocacy.org/blog/industry-voices-caution-against-dojs-plan-to-force-sale-of-chrome)
- [Industry Voices Caution Against DOJ’s Plan to Force Sale Of Chrome](https://open-web-advocacy.org/ja/blog/industry-voices-caution-against-dojs-plan-to-force-sale-of-chrome)
- [Industry Voices Caution Against DOJ’s Plan to Force Sale Of Chrome](https://open-web-advocacy.org/es/blog/industry-voices-caution-against-dojs-plan-to-force-sale-of-chrome)

### 45. 3 items; pair scores 100.0%–100.0% (mean 100.0%, 3 qualifying pairs)

Feed: <https://open-web-advocacy.org/feed.xml>

- [Is It Worth Killing Mozilla to Shave Off Less Than 1% From Google’s Market Share?](https://open-web-advocacy.org/blog/is-it-worth-killing-mozilla-to-shave-off-less-than-1-percent-from-googles-market-share)
- [Is It Worth Killing Mozilla to Shave Off Less Than 1% From Google’s Market Share?](https://open-web-advocacy.org/ja/blog/is-it-worth-killing-mozilla-to-shave-off-less-than-1-percent-from-googles-market-share)
- [Is It Worth Killing Mozilla to Shave Off Less Than 1% From Google’s Market Share?](https://open-web-advocacy.org/es/blog/is-it-worth-killing-mozilla-to-shave-off-less-than-1-percent-from-googles-market-share)

### 46. 3 items; pair scores 100.0%–100.0% (mean 100.0%, 3 qualifying pairs)

Feed: <https://open-web-advocacy.org/feed.xml>

- [UK Regulator's Final Verdict: Apple’s Browser Engine Ban Harms Competition](https://open-web-advocacy.org/blog/uk-regulators-final-verdict--apples-browser-engine-ban-harms-competition)
- [UK Regulator's Final Verdict: Apple’s Browser Engine Ban Harms Competition](https://open-web-advocacy.org/ja/blog/uk-regulators-final-verdict--apples-browser-engine-ban-harms-competition)
- [UK Regulator's Final Verdict: Apple’s Browser Engine Ban Harms Competition](https://open-web-advocacy.org/es/blog/uk-regulators-final-verdict--apples-browser-engine-ban-harms-competition)

### 47. 3 items; pair scores 100.0%–100.0% (mean 100.0%, 3 qualifying pairs)

Feed: <https://open-web-advocacy.org/feed.xml>

- [SLAP and FLOP: Apple's Lack of Full Site Isolation and iOS Browser Ban Puts Users at Risk](https://open-web-advocacy.org/blog/slap-and-flop--apples-lack-of-full-site-isolation-and-ios-browser-ban-puts-users-at-risk)
- [SLAP and FLOP: Apple's Lack of Full Site Isolation and iOS Browser Ban Puts Users at Risk](https://open-web-advocacy.org/ja/blog/slap-and-flop--apples-lack-of-full-site-isolation-and-ios-browser-ban-puts-users-at-risk)
- [SLAP and FLOP: Apple's Lack of Full Site Isolation and iOS Browser Ban Puts Users at Risk](https://open-web-advocacy.org/es/blog/slap-and-flop--apples-lack-of-full-site-isolation-and-ios-browser-ban-puts-users-at-risk)

### 48. 3 items; pair scores 100.0%–100.0% (mean 100.0%, 3 qualifying pairs)

Feed: <https://open-web-advocacy.org/feed.xml>

- [Digital Markets Act: Europe’s Digital Competitiveness at Stake](https://open-web-advocacy.org/blog/digital-markets-act--europes-digital-competitiveness-at-stake)
- [Digital Markets Act: Europe’s Digital Competitiveness at Stake](https://open-web-advocacy.org/ja/blog/digital-markets-act--europes-digital-competitiveness-at-stake)
- [Digital Markets Act: Europe’s Digital Competitiveness at Stake](https://open-web-advocacy.org/es/blog/digital-markets-act--europes-digital-competitiveness-at-stake)

### 49. 3 items; pair scores 100.0%–100.0% (mean 100.0%, 3 qualifying pairs)

Feed: <https://open-web-advocacy.org/feed.xml>

- [UK Launches Investigation into Apple and Google under the DMCC](https://open-web-advocacy.org/blog/uk-launches-investigation-into-apple-and-google-under-dmcc)
- [UK Launches Investigation into Apple and Google under the DMCC](https://open-web-advocacy.org/ja/blog/uk-launches-investigation-into-apple-and-google-under-dmcc)
- [UK Launches Investigation into Apple and Google under the DMCC](https://open-web-advocacy.org/es/blog/uk-launches-investigation-into-apple-and-google-under-dmcc)

### 50. 3 items; pair scores 100.0%–100.0% (mean 100.0%, 3 qualifying pairs)

Feed: <https://open-web-advocacy.org/feed.xml>

- [iOS age restriction blocks all browsers except Safari, breaks choice screen](https://open-web-advocacy.org/blog/ios-age-restriction-blocks-all-browsers-except-safari-breaks-choice-screen)
- [iOS age restriction blocks all browsers except Safari, breaks choice screen](https://open-web-advocacy.org/ja/blog/ios-age-restriction-blocks-all-browsers-except-safari-breaks-choice-screen)
- [iOS age restriction blocks all browsers except Safari, breaks choice screen](https://open-web-advocacy.org/es/blog/ios-age-restriction-blocks-all-browsers-except-safari-breaks-choice-screen)

### 51. 3 items; pair scores 100.0%–100.0% (mean 100.0%, 3 qualifying pairs)

Feed: <https://open-web-advocacy.org/feed.xml>

- [Apple implements six of OWA's DMA compliance requests](https://open-web-advocacy.org/blog/apple-implements-six-of-owas-dma-compliance-requests)
- [Apple implements six of OWA's DMA compliance requests](https://open-web-advocacy.org/ja/blog/apple-implements-six-of-owas-dma-compliance-requests)
- [Apple implements six of OWA's DMA compliance requests](https://open-web-advocacy.org/es/blog/apple-implements-six-of-owas-dma-compliance-requests)

### 52. 3 items; pair scores 100.0%–100.0% (mean 100.0%, 3 qualifying pairs)

Feed: <https://open-web-advocacy.org/feed.xml>

- [It's time for a fairer, more competitive app ecosystem](https://open-web-advocacy.org/blog/its-time-for-a-fairer-more-competitive-app-ecosystem)
- [It's time for a fairer, more competitive app ecosystem](https://open-web-advocacy.org/ja/blog/its-time-for-a-fairer-more-competitive-app-ecosystem)
- [It's time for a fairer, more competitive app ecosystem](https://open-web-advocacy.org/es/blog/its-time-for-a-fairer-more-competitive-app-ecosystem)

### 53. 3 items; pair scores 100.0%–100.0% (mean 100.0%, 3 qualifying pairs)

Feed: <https://open-web-advocacy.org/feed.xml>

- [Interop 2025 must drop secret vetos](https://open-web-advocacy.org/blog/interop-2025-must-drop-secret-vetos)
- [Interop 2025 must drop secret vetos](https://open-web-advocacy.org/ja/blog/interop-2025-must-drop-secret-vetos)
- [Interop 2025 must drop secret vetos](https://open-web-advocacy.org/es/blog/interop-2025-must-drop-secret-vetos)

### 54. 3 items; pair scores 100.0%–100.0% (mean 100.0%, 3 qualifying pairs)

Feed: <https://open-web-advocacy.org/feed.xml>

- [Webventures: An Abridged History of Safari Showstoppers](https://open-web-advocacy.org/blog/webventures-an-abridged-history-of-safari-showstoppers)
- [Webventures: An Abridged History of Safari Showstoppers](https://open-web-advocacy.org/ja/blog/webventures-an-abridged-history-of-safari-showstoppers)
- [Webventures: An Abridged History of Safari Showstoppers](https://open-web-advocacy.org/es/blog/webventures-an-abridged-history-of-safari-showstoppers)

### 55. 3 items; pair scores 100.0%–100.0% (mean 100.0%, 3 qualifying pairs)

Feed: <https://open-web-advocacy.org/feed.xml>

- [Google must share the ability to install Web Apps in Android](https://open-web-advocacy.org/blog/google-must-share-the-ability-to-install-web-apps-in-android)
- [Google must share the ability to install Web Apps in Android](https://open-web-advocacy.org/ja/blog/google-must-share-the-ability-to-install-web-apps-in-android)
- [Google must share the ability to install Web Apps in Android](https://open-web-advocacy.org/es/blog/google-must-share-the-ability-to-install-web-apps-in-android)

### 56. 3 items; pair scores 100.0%–100.0% (mean 100.0%, 3 qualifying pairs)

Feed: <https://open-web-advocacy.org/feed.xml>

- [Apple appears to mislead UK Regulator over deceptive default browser user interface](https://open-web-advocacy.org/blog/apple-appears-to-mislead-uk-regulator-over-deceptive-default-browser-user-interface)
- [Apple appears to mislead UK Regulator over deceptive default browser user interface](https://open-web-advocacy.org/ja/blog/apple-appears-to-mislead-uk-regulator-over-deceptive-default-browser-user-interface)
- [Apple appears to mislead UK Regulator over deceptive default browser user interface](https://open-web-advocacy.org/es/blog/apple-appears-to-mislead-uk-regulator-over-deceptive-default-browser-user-interface)

### 57. 3 items; pair scores 100.0%–100.0% (mean 100.0%, 3 qualifying pairs)

Feed: <https://open-web-advocacy.org/feed.xml>

- [Browser and Cloud Gaming Market Investigation Update](https://open-web-advocacy.org/blog/uk-cma-browser-cloud-gaming-progress-report)
- [Browser and Cloud Gaming Market Investigation Update](https://open-web-advocacy.org/ja/blog/uk-cma-browser-cloud-gaming-progress-report)
- [Browser and Cloud Gaming Market Investigation Update](https://open-web-advocacy.org/es/blog/uk-cma-browser-cloud-gaming-progress-report)

### 58. 3 items; pair scores 100.0%–100.0% (mean 100.0%, 3 qualifying pairs)

Feed: <https://open-web-advocacy.org/feed.xml>

- [Apple DMA Review](https://open-web-advocacy.org/blog/apple-dma-review)
- [Apple DMA Review](https://open-web-advocacy.org/ja/blog/apple-dma-review)
- [Apple DMA Review](https://open-web-advocacy.org/es/blog/apple-dma-review)

### 59. 3 items; pair scores 100.0%–100.0% (mean 100.0%, 3 qualifying pairs)

Feed: <https://open-web-advocacy.org/feed.xml>

- [Japan ends the #AppleBrowserBan](https://open-web-advocacy.org/blog/japan-ends-the-apple-browser-ban)
- [Japan ends the #AppleBrowserBan](https://open-web-advocacy.org/ja/blog/japan-ends-the-apple-browser-ban)
- [Japan ends the #AppleBrowserBan](https://open-web-advocacy.org/es/blog/japan-ends-the-apple-browser-ban)

### 60. 3 items; pair scores 100.0%–100.0% (mean 100.0%, 3 qualifying pairs)

Feed: <https://open-web-advocacy.org/feed.xml>

- [UK passes Digital Markets, Competition and Consumers Bill](https://open-web-advocacy.org/blog/uk-passes-dmcc)
- [UK passes Digital Markets, Competition and Consumers Bill](https://open-web-advocacy.org/ja/blog/uk-passes-dmcc)
- [UK passes Digital Markets, Competition and Consumers Bill](https://open-web-advocacy.org/es/blog/uk-passes-dmcc)

### 61. 3 items; pair scores 100.0%–100.0% (mean 100.0%, 3 qualifying pairs)

Feed: <https://open-web-advocacy.org/feed.xml>

- [Apple's one weird trick to stop you changing your default browser](https://open-web-advocacy.org/blog/apples-one-weird-trick-to-stop-you-changing-your-default-browser)
- [Apple's one weird trick to stop you changing your default browser](https://open-web-advocacy.org/ja/blog/apples-one-weird-trick-to-stop-you-changing-your-default-browser)
- [Apple's one weird trick to stop you changing your default browser](https://open-web-advocacy.org/es/blog/apples-one-weird-trick-to-stop-you-changing-your-default-browser)

### 62. 3 items; pair scores 100.0%–100.0% (mean 100.0%, 3 qualifying pairs)

Feed: <https://open-web-advocacy.org/feed.xml>

- [US Department of Justice files Antitrust lawsuit Against Apple](https://open-web-advocacy.org/blog/us-doj-files-apple-antitrust-case)
- [US Department of Justice files Antitrust lawsuit Against Apple](https://open-web-advocacy.org/ja/blog/us-doj-files-apple-antitrust-case)
- [US Department of Justice files Antitrust lawsuit Against Apple](https://open-web-advocacy.org/es/blog/us-doj-files-apple-antitrust-case)

### 63. 3 items; pair scores 100.0%–100.0% (mean 100.0%, 3 qualifying pairs)

Feed: <https://open-web-advocacy.org/feed.xml>

- [The Digital Markets Act is in force! What happens now?](https://open-web-advocacy.org/blog/the-digital-markets-act-is-in-force-what-happens-now)
- [The Digital Markets Act is in force! What happens now?](https://open-web-advocacy.org/ja/blog/the-digital-markets-act-is-in-force-what-happens-now)
- [The Digital Markets Act is in force! What happens now?](https://open-web-advocacy.org/es/blog/the-digital-markets-act-is-in-force-what-happens-now)

### 64. 3 items; pair scores 100.0%–100.0% (mean 100.0%, 3 qualifying pairs)

Feed: <https://open-web-advocacy.org/feed.xml>

- [Apple backs off killing web apps, but the fight continues](https://open-web-advocacy.org/blog/apple-backs-off-killing-web-apps)
- [Apple backs off killing web apps, but the fight continues](https://open-web-advocacy.org/ja/blog/apple-backs-off-killing-web-apps)
- [Apple backs off killing web apps, but the fight continues](https://open-web-advocacy.org/es/blog/apple-backs-off-killing-web-apps)

### 65. 3 items; pair scores 100.0%–100.0% (mean 100.0%, 3 qualifying pairs)

Feed: <https://open-web-advocacy.org/feed.xml>

- [It’s Official, Apple Kills Web Apps in the EU](https://open-web-advocacy.org/blog/its-official-apple-kills-web-apps-in-the-eu)
- [It’s Official, Apple Kills Web Apps in the EU](https://open-web-advocacy.org/ja/blog/its-official-apple-kills-web-apps-in-the-eu)
- [It’s Official, Apple Kills Web Apps in the EU](https://open-web-advocacy.org/es/blog/its-official-apple-kills-web-apps-in-the-eu)

### 66. 3 items; pair scores 100.0%–100.0% (mean 100.0%, 3 qualifying pairs)

Feed: <https://open-web-advocacy.org/feed.xml>

- [Apple on course to break all Web Apps in EU within 20 days](https://open-web-advocacy.org/blog/apple-on-course-to-break-all-web-apps-in-eu-within-20-days)
- [Apple on course to break all Web Apps in EU within 20 days](https://open-web-advocacy.org/ja/blog/apple-on-course-to-break-all-web-apps-in-eu-within-20-days)
- [Apple on course to break all Web Apps in EU within 20 days](https://open-web-advocacy.org/es/blog/apple-on-course-to-break-all-web-apps-in-eu-within-20-days)

### 67. 3 items; pair scores 100.0%–100.0% (mean 100.0%, 3 qualifying pairs)

Feed: <https://open-web-advocacy.org/feed.xml>

- [Did Apple just break Web Apps in iOS 17.4 Beta (EU)?](https://open-web-advocacy.org/blog/did-apple-just-break-web-apps-in-ios17.4-beta-eu)
- [Did Apple just break Web Apps in iOS 17.4 Beta (EU)?](https://open-web-advocacy.org/ja/blog/did-apple-just-break-web-apps-in-ios17.4-beta-eu)
- [Did Apple just break Web Apps in iOS 17.4 Beta (EU)?](https://open-web-advocacy.org/es/blog/did-apple-just-break-web-apps-in-ios17.4-beta-eu)

### 68. 3 items; pair scores 100.0%–100.0% (mean 100.0%, 3 qualifying pairs)

Feed: <https://open-web-advocacy.org/feed.xml>

- [Web Developers React to Apple’s DMA Compliance Proposal](https://open-web-advocacy.org/blog/developers-react-apple-eu-dma-compliance)
- [Web Developers React to Apple’s DMA Compliance Proposal](https://open-web-advocacy.org/ja/blog/developers-react-apple-eu-dma-compliance)
- [Web Developers React to Apple’s DMA Compliance Proposal](https://open-web-advocacy.org/es/blog/developers-react-apple-eu-dma-compliance)

### 69. 3 items; pair scores 100.0%–100.0% (mean 100.0%, 3 qualifying pairs)

Feed: <https://open-web-advocacy.org/feed.xml>

- [OWA’s Review of Apple’s DMA Compliance Proposal for the Web](https://open-web-advocacy.org/blog/owa-review-apple-dma-compliance-for-web)
- [OWA’s Review of Apple’s DMA Compliance Proposal for the Web](https://open-web-advocacy.org/ja/blog/owa-review-apple-dma-compliance-for-web)
- [OWA’s Review of Apple’s DMA Compliance Proposal for the Web](https://open-web-advocacy.org/es/blog/owa-review-apple-dma-compliance-for-web)

### 70. 3 items; pair scores 100.0%–100.0% (mean 100.0%, 3 qualifying pairs)

Feed: <https://open-web-advocacy.org/feed.xml>

- [Apple's plan to allow browser competition dubbed unworkable](https://open-web-advocacy.org/blog/apple-dma-changes)
- [Apple's plan to allow browser competition dubbed unworkable](https://open-web-advocacy.org/ja/blog/apple-dma-changes)
- [Apple's plan to allow browser competition dubbed unworkable](https://open-web-advocacy.org/es/blog/apple-dma-changes)

### 71. 3 items; pair scores 100.0%–100.0% (mean 100.0%, 3 qualifying pairs)

Feed: <https://open-web-advocacy.org/feed.xml>

- [Apple files another challenge to the EU Digital Markets Act](https://open-web-advocacy.org/blog/apple-filing-eu-appstores)
- [Apple files another challenge to the EU Digital Markets Act](https://open-web-advocacy.org/ja/blog/apple-filing-eu-appstores)
- [Apple files another challenge to the EU Digital Markets Act](https://open-web-advocacy.org/es/blog/apple-filing-eu-appstores)

### 72. 3 items; pair scores 100.0%–100.0% (mean 100.0%, 3 qualifying pairs)

Feed: <https://open-web-advocacy.org/feed.xml>

- [Open Web Advocacy 2023 in Review](https://open-web-advocacy.org/blog/owa-2023-review)
- [Open Web Advocacy 2023 in Review](https://open-web-advocacy.org/ja/blog/owa-2023-review)
- [Open Web Advocacy 2023 in Review](https://open-web-advocacy.org/es/blog/owa-2023-review)

### 73. 3 items; pair scores 100.0%–100.0% (mean 100.0%, 3 qualifying pairs)

Feed: <https://open-web-advocacy.org/feed.xml>

- [UK browser investigation to restart Jan 24 after Apple fail to appeal](https://open-web-advocacy.org/blog/cma-reopens-investigation-into-apple)
- [UK browser investigation to restart Jan 24 after Apple fail to appeal](https://open-web-advocacy.org/ja/blog/cma-reopens-investigation-into-apple)
- [UK browser investigation to restart Jan 24 after Apple fail to appeal](https://open-web-advocacy.org/es/blog/cma-reopens-investigation-into-apple)

### 74. 3 items; pair scores 100.0%–100.0% (mean 100.0%, 3 qualifying pairs)

Feed: <https://open-web-advocacy.org/feed.xml>

- [New Digital Competition Laws for Australia](https://open-web-advocacy.org/blog/new-digital-competition-laws-for-australia)
- [New Digital Competition Laws for Australia](https://open-web-advocacy.org/ja/blog/new-digital-competition-laws-for-australia)
- [New Digital Competition Laws for Australia](https://open-web-advocacy.org/es/blog/new-digital-competition-laws-for-australia)

### 75. 3 items; pair scores 100.0%–100.0% (mean 100.0%, 3 qualifying pairs)

Feed: <https://open-web-advocacy.org/feed.xml>

- [iOS Safari zero-day security bugs and how browser choice affects user safety](https://open-web-advocacy.org/blog/security-updates-browser-choice)
- [iOS Safari zero-day security bugs and how browser choice affects user safety](https://open-web-advocacy.org/ja/blog/security-updates-browser-choice)
- [iOS Safari zero-day security bugs and how browser choice affects user safety](https://open-web-advocacy.org/es/blog/security-updates-browser-choice)

### 76. 3 items; pair scores 100.0%–100.0% (mean 100.0%, 3 qualifying pairs)

Feed: <https://open-web-advocacy.org/feed.xml>

- [Apple loses on Appeal, CMA can restart investigation into browsers](https://open-web-advocacy.org/blog/apple-loses-on-appeal)
- [Apple loses on Appeal, CMA can restart investigation into browsers](https://open-web-advocacy.org/ja/blog/apple-loses-on-appeal)
- [Apple loses on Appeal, CMA can restart investigation into browsers](https://open-web-advocacy.org/es/blog/apple-loses-on-appeal)

### 77. 3 items; pair scores 100.0%–100.0% (mean 100.0%, 3 qualifying pairs)

Feed: <https://open-web-advocacy.org/feed.xml>

- [OWA submits response to EU DMA investigation into Apple iPadOS](https://open-web-advocacy.org/blog/owa-eu-dma-submission-apple-ipados)
- [OWA submits response to EU DMA investigation into Apple iPadOS](https://open-web-advocacy.org/ja/blog/owa-eu-dma-submission-apple-ipados)
- [OWA submits response to EU DMA investigation into Apple iPadOS](https://open-web-advocacy.org/es/blog/owa-eu-dma-submission-apple-ipados)

### 78. 3 items; pair scores 100.0%–100.0% (mean 100.0%, 3 qualifying pairs)

Feed: <https://open-web-advocacy.org/feed.xml>

- [Why is browser choice vital for the future of the Open Web?](https://open-web-advocacy.org/blog/why-browser-choice-matters)
- [Why is browser choice vital for the future of the Open Web?](https://open-web-advocacy.org/ja/blog/why-browser-choice-matters)
- [Why is browser choice vital for the future of the Open Web?](https://open-web-advocacy.org/es/blog/why-browser-choice-matters)

### 79. 3 items; pair scores 100.0%–100.0% (mean 100.0%, 3 qualifying pairs)

Feed: <https://open-web-advocacy.org/feed.xml>

- [News from the NTIA report on mobile app ecosystems](https://open-web-advocacy.org/blog/ntia-report-feb-22)
- [News from the NTIA report on mobile app ecosystems](https://open-web-advocacy.org/ja/blog/ntia-report-feb-22)
- [News from the NTIA report on mobile app ecosystems](https://open-web-advocacy.org/es/blog/ntia-report-feb-22)

### 80. 3 items; pair scores 100.0%–100.0% (mean 100.0%, 3 qualifying pairs)

Feed: <https://open-web-advocacy.org/feed.xml>

- [Australian Government considering ACCC proposed anti-competitive conduct legislation changes](https://open-web-advocacy.org/blog/australia-accc-nov-22)
- [Australian Government considering ACCC proposed anti-competitive conduct legislation changes](https://open-web-advocacy.org/ja/blog/australia-accc-nov-22)
- [Australian Government considering ACCC proposed anti-competitive conduct legislation changes](https://open-web-advocacy.org/es/blog/australia-accc-nov-22)

### 81. 3 items; pair scores 100.0%–100.0% (mean 100.0%, 3 qualifying pairs)

Feed: <https://open-web-advocacy.org/feed.xml>

- [OWA updates on progress with UK and EU digital competition regulations](https://open-web-advocacy.org/blog/cma-dma-nov-22)
- [OWA updates on progress with UK and EU digital competition regulations](https://open-web-advocacy.org/ja/blog/cma-dma-nov-22)
- [OWA updates on progress with UK and EU digital competition regulations](https://open-web-advocacy.org/es/blog/cma-dma-nov-22)

### 82. 3 items; pair scores 100.0%–100.0% (mean 100.0%, 3 qualifying pairs)

Feed: <https://open-web-advocacy.org/feed.xml>

- [OWA response to the CMA interim report](https://open-web-advocacy.org/blog/our-response-cma-interim-report)
- [OWA response to the CMA interim report](https://open-web-advocacy.org/ja/blog/our-response-cma-interim-report)
- [OWA response to the CMA interim report](https://open-web-advocacy.org/es/blog/our-response-cma-interim-report)

### 83. 3 items; pair scores 100.0%–100.0% (mean 100.0%, 3 qualifying pairs)

Feed: <https://www.efinancialcareers.com/feed/syndication/rss.xml>

- [How to get a job in a hedge fund](https://www.efinancialcareers.com/news/how-to-get-a-hedge-fund-job)
- [How to get a job in a hedge fund](https://www.efinancialcareers.com/news/finance/how-to-get-a-hedge-fund-job)
- [How to get a job in a hedge fund](https://www.efinancialcareers.com/news/2022/03/how-to-get-a-hedge-fund-job)

### 84. 3 items; pair scores 100.0%–100.0% (mean 100.0%, 3 qualifying pairs)

Feed: <https://open-web-advocacy.org/feed.xml>

- [Open Web Advocacy 2025 in Review](https://open-web-advocacy.org/blog/owa-2025-review)
- [Open Web Advocacy 2025 in Review](https://open-web-advocacy.org/ja/blog/owa-2025-review)
- [Open Web Advocacy 2025 in Review](https://open-web-advocacy.org/es/blog/owa-2025-review)

### 85. 3 items; pair scores 100.0%–100.0% (mean 100.0%, 3 qualifying pairs)

Feed: <https://open-web-advocacy.org/feed.xml>

- [Break Google’s Search Monopoly without Breaking the Web](https://open-web-advocacy.org/blog/break-googles-search-monopoly-without-breaking-the-web)
- [Break Google’s Search Monopoly without Breaking the Web](https://open-web-advocacy.org/ja/blog/break-googles-search-monopoly-without-breaking-the-web)
- [Break Google’s Search Monopoly without Breaking the Web](https://open-web-advocacy.org/es/blog/break-googles-search-monopoly-without-breaking-the-web)

### 86. 3 items; pair scores 99.9%–100.0% (mean 100.0%, 3 qualifying pairs)

Feed: <https://open-web-advocacy.org/feed.xml>

- [Our Submission to the CMA on Apple’s iOS Interoperability Commitments](https://open-web-advocacy.org/blog/our-submission-to-the-cma-on-apples-ios-interoperability-commitments)
- [Our Submission to the CMA on Apple’s iOS Interoperability Commitments](https://open-web-advocacy.org/ja/blog/our-submission-to-the-cma-on-apples-ios-interoperability-commitments)
- [Our Submission to the CMA on Apple’s iOS Interoperability Commitments](https://open-web-advocacy.org/es/blog/our-submission-to-the-cma-on-apples-ios-interoperability-commitments)

### 87. 3 items; pair scores 99.8%–100.0% (mean 99.9%, 3 qualifying pairs)

Feed: <https://open-web-advocacy.org/feed.xml>

- [OWA at the EU Parliament DMA Working Group](https://open-web-advocacy.org/blog/owa-at-the-eu-parliament-dma-working-group)
- [OWA at the EU Parliament DMA Working Group](https://open-web-advocacy.org/ja/blog/owa-at-the-eu-parliament-dma-working-group)
- [OWA at the EU Parliament DMA Working Group](https://open-web-advocacy.org/es/blog/owa-at-the-eu-parliament-dma-working-group)

### 88. 3 items; pair scores 99.8%–100.0% (mean 99.9%, 3 qualifying pairs)

Feed: <https://open-web-advocacy.org/feed.xml>

- [Apple's Browser Engine Ban Persists, Even Under the DMA](https://open-web-advocacy.org/blog/apples-browser-engine-ban-persists-even-under-the-dma)
- [Apple's Browser Engine Ban Persists, Even Under the DMA](https://open-web-advocacy.org/ja/blog/apples-browser-engine-ban-persists-even-under-the-dma)
- [Apple's Browser Engine Ban Persists, Even Under the DMA](https://open-web-advocacy.org/es/blog/apples-browser-engine-ban-persists-even-under-the-dma)

### 89. 3 items; pair scores 99.7%–100.0% (mean 99.8%, 3 qualifying pairs)

Feed: <https://open-web-advocacy.org/feed.xml>

- [Open Web Advocacy 2024 in Review](https://open-web-advocacy.org/blog/owa-2024-review)
- [Open Web Advocacy 2024 in Review](https://open-web-advocacy.org/ja/blog/owa-2024-review)
- [Open Web Advocacy 2024 in Review](https://open-web-advocacy.org/es/blog/owa-2024-review)

### 90. 3 items; pair scores 99.7%–100.0% (mean 99.8%, 3 qualifying pairs)

Feed: <https://open-web-advocacy.org/feed.xml>

- [UK’s Browser and Cloud Investigation may fail to allow Web App competition](https://open-web-advocacy.org/blog/uk-browser-and-cloud-investigation-may-fail-to-allow-web-app-competition)
- [UK’s Browser and Cloud Investigation may fail to allow Web App competition](https://open-web-advocacy.org/ja/blog/uk-browser-and-cloud-investigation-may-fail-to-allow-web-app-competition)
- [UK’s Browser and Cloud Investigation may fail to allow Web App competition](https://open-web-advocacy.org/es/blog/uk-browser-and-cloud-investigation-may-fail-to-allow-web-app-competition)

### 91. 3 items; pair scores 99.6%–100.0% (mean 99.7%, 3 qualifying pairs)

Feed: <https://open-web-advocacy.org/feed.xml>

- [Tim Berners-Lee On Apple’s Browser Engine Ban and Web Apps](https://open-web-advocacy.org/blog/tim-berners-lee-on-apples-browser-engine-ban-and-web-apps)
- [Tim Berners-Lee On Apple’s Browser Engine Ban and Web Apps](https://open-web-advocacy.org/ja/blog/tim-berners-lee-on-apples-browser-engine-ban-and-web-apps)
- [Tim Berners-Lee On Apple’s Browser Engine Ban and Web Apps](https://open-web-advocacy.org/es/blog/tim-berners-lee-on-apples-browser-engine-ban-and-web-apps)

### 92. 3 items; pair scores 99.5%–100.0% (mean 99.7%, 3 qualifying pairs)

Feed: <https://open-web-advocacy.org/feed.xml>

- [Google's Hotseat Hypocrisy](https://open-web-advocacy.org/blog/googles-hotseat-hypocrisy)
- [Google's Hotseat Hypocrisy](https://open-web-advocacy.org/ja/blog/googles-hotseat-hypocrisy)
- [Google's Hotseat Hypocrisy](https://open-web-advocacy.org/es/blog/googles-hotseat-hypocrisy)

### 93. 3 items; pair scores 99.1%–100.0% (mean 99.4%, 3 qualifying pairs)

Feed: <https://open-web-advocacy.org/feed.xml>

- [In-App Browsers: The worst erosion of user choice you haven't heard of](https://open-web-advocacy.org/blog/in-app-browsers-the-worst-erosion-of-user-choice-you-havent-heard-of)
- [In-App Browsers: The worst erosion of user choice you haven't heard of](https://open-web-advocacy.org/ja/blog/in-app-browsers-the-worst-erosion-of-user-choice-you-havent-heard-of)
- [In-App Browsers: The worst erosion of user choice you haven't heard of](https://open-web-advocacy.org/es/blog/in-app-browsers-the-worst-erosion-of-user-choice-you-havent-heard-of)

### 94. 3 items; pair scores 99.2%–99.4% (mean 99.3%, 3 qualifying pairs)

Feed: <https://metr.org/feed.xml>

- [对 OpenAI / Hugging Face 入侵事件中智能体行为、推理与协作的简要独立调查](https://metr.org/zh-hans/blog/2026-08-26-openai-hugging-face-incident-investigation)
- [Breve investigación independiente sobre el comportamiento, el razonamiento y la colaboración de los agentes en el incidente de hackeo de OpenAI / Hugging Face](https://metr.org/es/blog/2026-08-26-openai-hugging-face-incident-investigation)
- [Brief independent investigation of agents’ behavior, reasoning and collaboration in the OpenAI / Hugging Face hacking incident](https://metr.org/blog/2026-08-26-openai-hugging-face-incident-investigation)

### 95. 3 items; pair scores 98.9%–100.0% (mean 99.3%, 3 qualifying pairs)

Feed: <https://open-web-advocacy.org/feed.xml>

- [Apple adopts 6 of OWA's Choice Architecture Recommendations](https://open-web-advocacy.org/blog/apple-adopts-6-owa-choice-architecture-recommendations)
- [Apple adopts 6 of OWA's Choice Architecture Recommendations](https://open-web-advocacy.org/ja/blog/apple-adopts-6-owa-choice-architecture-recommendations)
- [Apple adopts 6 of OWA's Choice Architecture Recommendations](https://open-web-advocacy.org/es/blog/apple-adopts-6-owa-choice-architecture-recommendations)

### 96. 3 items; pair scores 98.9%–99.6% (mean 99.2%, 3 qualifying pairs)

Feed: <https://www.bettedangerous.com/feed>

- [REMINDER: Bette's Sunday Speakeasy with Monique Camarra on Canada and EU’s ‘Road Map for the Future’](https://www.bettedangerous.com/p/reminder-bettes-sunday-speakeasy-2a7)
- [CORRECTED REGISTRATION: Bette's Sunday Speakeasy with Monique Camarra on Canada and EU’s ‘Road Map for the Future’](https://www.bettedangerous.com/p/corrected-registration-bettes-sunday)
- [REGISTER: Bette's Sunday Speakeasy with Monique Camarra on Canada and EU’s ‘Road Map for the Future’](https://www.bettedangerous.com/p/register-bettes-sunday-speakeasy-627)

### 97. 3 items; pair scores 98.8%–98.8% (mean 98.8%, 3 qualifying pairs)

Feed: <https://rss.art19.com/tim-ferriss-show>

- [#556: The Incredible Kyle Maynard — Fear{less} with Tim Ferriss](https://rss.art19.com/episodes/332faa1c-ac4d-4d22-864d-191154a9ba5b.mp3?rss_browser=bahjigtdahjvbwugogzfva%3d%3d--d05363d83ce333c74f32188013892b2863ad051c)
- [#551: TOMS Founder Blake Mycoskie — Fear{less} with Tim Ferriss](https://rss.art19.com/episodes/2f99c414-a7dd-4c02-8a7e-47e6cd50d6c8.mp3?rss_browser=bahjigtdahjvbwugogzfva%3d%3d--d05363d83ce333c74f32188013892b2863ad051c)
- [#546: Master Magician David Blaine — Fear{less} with Tim Ferriss](https://rss.art19.com/episodes/da39dce6-2f1d-4fc4-a374-dc637d4dada8.mp3?rss_browser=bahjigtdahjvbwugogzfva%3d%3d--d05363d83ce333c74f32188013892b2863ad051c)

### 98. 3 items; pair scores 97.9%–100.0% (mean 98.6%, 3 qualifying pairs)

Feed: <https://open-web-advocacy.org/feed.xml>

- [The Digital Markets Act Is Delivering Real Wins, But Not Yet for Browser Engines](https://open-web-advocacy.org/blog/the-digital-markets-act-is-delivering-real-wins-but-not-yet-for-browser-engines)
- [The Digital Markets Act Is Delivering Real Wins, But Not Yet for Browser Engines](https://open-web-advocacy.org/ja/blog/the-digital-markets-act-is-delivering-real-wins-but-not-yet-for-browser-engines)
- [The Digital Markets Act Is Delivering Real Wins, But Not Yet for Browser Engines](https://open-web-advocacy.org/es/blog/the-digital-markets-act-is-delivering-real-wins-but-not-yet-for-browser-engines)

### 99. 3 items; pair scores 97.8%–100.0% (mean 98.5%, 3 qualifying pairs)

Feed: <https://open-web-advocacy.org/feed.xml>

- [EU opens DMA investigations of Apple, Meta, Google](https://open-web-advocacy.org/blog/eu-opens-dma-investigations)
- [EU opens DMA investigations of Apple, Meta, Google](https://open-web-advocacy.org/ja/blog/eu-opens-dma-investigations)
- [EU opens DMA investigations of Apple, Meta, Google](https://open-web-advocacy.org/es/blog/eu-opens-dma-investigations)

### 100. 3 items; pair scores 97.6%–100.0% (mean 98.4%, 3 qualifying pairs)

Feed: <https://georgheiler.com/index.xml>

- [Cost efficient alternative to databricks lock-in](https://georgheiler.com/event/cost-efficient-alternative-to-databricks-lock-in)
- [Cost-Effective Big Data Orchestration Using Dagster: A Multi-Platform Approach](https://georgheiler.com/publication/cost-effective-big-data-orchestration-using-dagster-a-multi-platform-approach)
- [Cloud arbitrage for spark pipelines](https://georgheiler.com/event/cloud-arbitrage-for-spark-pipelines)

### 101. 3 items; pair scores 97.2%–100.0% (mean 98.2%, 3 qualifying pairs)

Feed: <http://feeds.thememorypalace.us/thememorypalace>

- [A White Horse](https://play.prx.org/listen?ge=prx_3_a66567d8-e5b5-4d2c-afe2-7ae3915d0c1c&uf=http%3a%2f%2ffeeds.thememorypalace.us%2fthememorypalace)
- [Episode 90: A White Horse](https://play.prx.org/listen?ge=prx_3_17e76b27-14a9-459f-8e12-8740e22b44f0&uf=http%3a%2f%2ffeeds.thememorypalace.us%2fthememorypalace)
- [Episode 90: A White Horse](https://play.prx.org/listen?ge=prx_3_14d444c2-bdfb-4fe9-9271-5a30bf982d26&uf=http%3a%2f%2ffeeds.thememorypalace.us%2fthememorypalace)

### 102. 3 items; pair scores 97.0%–98.6% (mean 98.0%, 3 qualifying pairs)

Feed: <https://www.sdr-radio.com/feed/rss2>

- [Simon's World Map 1.6.2](https://www.sdr-radio.com/simon-s-world-map-1-6-2)
- [Simon's World Map 1.6.1](https://www.sdr-radio.com/simon-s-world-map-1-6-1)
- [Simon's World Map 1.6](https://www.sdr-radio.com/simon-s-world-map-1-6)

### 103. 3 items; pair scores 96.8%–100.0% (mean 97.9%, 3 qualifying pairs)

Feed: <https://open-web-advocacy.org/feed.xml>

- [Q&A with Simonetta Vezzoso: The Open Web, Apple, and the DMA](https://open-web-advocacy.org/blog/question-and-answer-with-simonetta-vezzoso--the-open-web--apple-and-the-dma)
- [Q&A with Simonetta Vezzoso: The Open Web, Apple, and the DMA](https://open-web-advocacy.org/ja/blog/question-and-answer-with-simonetta-vezzoso--the-open-web--apple-and-the-dma)
- [Q&A with Simonetta Vezzoso: The Open Web, Apple, and the DMA](https://open-web-advocacy.org/es/blog/question-and-answer-with-simonetta-vezzoso--the-open-web--apple-and-the-dma)

### 104. 3 items; pair scores 96.4%–100.0% (mean 97.6%, 3 qualifying pairs)

Feed: <https://open-web-advocacy.org/feed.xml>

- [Can Perplexity Afford to Fund the Web? The $34.5 Billion-Dollar Question](https://open-web-advocacy.org/blog/can-perplexity-afford-to-fund-the-web)
- [Can Perplexity Afford to Fund the Web? The $34.5 Billion-Dollar Question](https://open-web-advocacy.org/ja/blog/can-perplexity-afford-to-fund-the-web)
- [Can Perplexity Afford to Fund the Web? The $34.5 Billion-Dollar Question](https://open-web-advocacy.org/es/blog/can-perplexity-afford-to-fund-the-web)

### 105. 3 items; pair scores 96.2%–99.7% (mean 97.5%, 3 qualifying pairs)

Feed: <https://rss.art19.com/tim-ferriss-show>

- [Ep 32: Tracy DiNunzio (Part 3), Founder of Tradesy, on Rapid Growth and Rapid Learning](https://rss.art19.com/episodes/8dcfe7e1-1d72-4441-ad52-ba619e687a89.mp3?rss_browser=bahjigtdahjvbwugogzfva%3d%3d--d05363d83ce333c74f32188013892b2863ad051c)
- [Ep 31: Tracy DiNunzio (Part 2), Founder of Tradesy, on Rapid Growth and Rapid Learning](https://rss.art19.com/episodes/ad384750-d57b-4102-af0b-fc50a3a86168.mp3?rss_browser=bahjigtdahjvbwugogzfva%3d%3d--d05363d83ce333c74f32188013892b2863ad051c)
- [Ep 30: Tracy DiNunzio, Founder of Tradesy, on High-Velocity Growth and Tactics](https://rss.art19.com/episodes/2aa7ad41-b106-4a74-825d-def9e7a5e489.mp3?rss_browser=bahjigtdahjvbwugogzfva%3d%3d--d05363d83ce333c74f32188013892b2863ad051c)

### 106. 3 items; pair scores 97.1%–98.1% (mean 97.5%, 3 qualifying pairs)

Feed: <https://pages.micahrl.com/ldapenforcer/index.xml>

- [ldapenforcer sync sync-group](https://pages.micahrl.com/ldapenforcer/docs/command/ldapenforcer_sync_sync-group)
- [ldapenforcer sync sync-person](https://pages.micahrl.com/ldapenforcer/docs/command/ldapenforcer_sync_sync-person)
- [ldapenforcer sync sync-svcacct](https://pages.micahrl.com/ldapenforcer/docs/command/ldapenforcer_sync_sync-svcacct)

### 107. 3 items; pair scores 95.9%–100.0% (mean 97.3%, 3 qualifying pairs)

Feed: <https://feeds.npr.org/510308/podcast.xml>

- [Liar, Liar, Liar](https://www.siriusxm.com)
- [Liar, Liar](https://www.siriusxm.com)
- [Ep. 66: Liar, Liar](https://www.siriusxm.com)

### 108. 3 items; pair scores 95.8%–100.0% (mean 97.2%, 3 qualifying pairs)

Feed: <https://open-web-advocacy.org/feed.xml>

- [Google Backs Down: Will Grant Hotseat in EU Browser Choice Screen](https://open-web-advocacy.org/blog/google-backs-down--will-grant-hotseat-in-eu-browser-choice-screen)
- [Google Backs Down: Will Grant Hotseat in EU Browser Choice Screen](https://open-web-advocacy.org/ja/blog/google-backs-down--will-grant-hotseat-in-eu-browser-choice-screen)
- [Google Backs Down: Will Grant Hotseat in EU Browser Choice Screen](https://open-web-advocacy.org/es/blog/google-backs-down--will-grant-hotseat-in-eu-browser-choice-screen)

### 109. 3 items; pair scores 96.4%–97.1% (mean 96.7%, 3 qualifying pairs)

Feed: <https://feeds.simplecast.com/dLRotFGk>

- [Coaching Yourself: Career Coaching Personas for Everyday Engineers, Part Three - Shoulder Socrates](http://www.developertea.com)
- [Coaching Yourself: Career Coaching Personas for Everyday Engineers, Part Two - The Overoptimizer](http://www.developertea.com)
- [Coaching Yourself: Career Coaching Personas for Everyday Engineers, Part One - The Available Manager](http://www.developertea.com)

### 110. 3 items; pair scores 94.3%–98.8% (mean 96.1%, 3 qualifying pairs)

Feed: <https://rss.art19.com/tim-ferriss-show>

- [Ep. 11: Drugs and the Meaning of Life](https://rss.art19.com/episodes/a7a81ce7-42ff-4fb1-a981-4b02b8fa680a.mp3?rss_browser=bahjigtdahjvbwugogzfva%3d%3d--d05363d83ce333c74f32188013892b2863ad051c)
- [Episode 9: The 9 Habits to Stop Now -- The Not-To-Do List](https://rss.art19.com/episodes/f782ad24-b42e-4ad6-b1b1-6aee2b086cce.mp3?rss_browser=bahjigtdahjvbwugogzfva%3d%3d--d05363d83ce333c74f32188013892b2863ad051c)
- [Episode 6: 6 Formulas for More Output and Less Overwhelm](https://rss.art19.com/episodes/b5a63fef-bebf-4085-ad69-dbf004d76674.mp3?rss_browser=bahjigtdahjvbwugogzfva%3d%3d--d05363d83ce333c74f32188013892b2863ad051c)

### 111. 3 items; pair scores 93.1%–98.0% (mean 95.3%, 3 qualifying pairs)

Feed: <https://highscalability.com/rss/>

- [Sponsored Post: G-Core Labs, Close, Wynter, Pinecone, Kinsta, Bridgecrew, IP2Location, StackHawk, InterviewCamp.io, Educative, Stream, Fauna, Triplebyte](https://highscalability.com/sponsored-post-g-core-labs-close-wynter-pinecone-kinsta-brid-65bcda83ffae163275932644)
- [Sponsored Post: G-Core Labs, Close, Wynter, Pinecone, Kinsta, Bridgecrew, IP2Location, StackHawk, InterviewCamp.io, Educative, Stream, Fauna, Triplebyte](https://highscalability.com/sponsored-post-g-core-labs-close-wynter-pinecone-kinsta-brid)
- [Sponsored Post: Close, Wynter, Pinecone, Kinsta, Bridgecrew, IP2Location, StackHawk, InterviewCamp.io, Educative, Stream, Fauna, Triplebyte](https://highscalability.com/sponsored-post-close-wynter-pinecone-kinsta-bridgecrew-ip2lo)

### 112. 3 items; pair scores 93.9%–94.6% (mean 94.2%, 2 qualifying pairs)

Feed: <https://blog.ifcomp.org/rss>

- [IFComp 2026 Now Accepting Intents &amp; Entries](https://blog.ifcomp.org/post/821438949798084608)
- [IFComp 2025 Now Accepting Intents &amp; Entries](https://blog.ifcomp.org/post/788067590629146624)
- [IFComp 2024 Now Accepting Intents &amp; Entries](https://blog.ifcomp.org/post/755083719507935232)

### 113. 3 items; pair scores 91.9%–96.8% (mean 93.6%, 3 qualifying pairs)

Feed: <https://www.whitehouse.gov/presidential-actions/feed/>

- [Excluding Certain Canadian Alcoholic Beverages from Importation into the United States in Response to Continued Discrimination Against the Commerce of the United States with Respect to Alcoholic Beverages](https://www.whitehouse.gov/presidential-actions/2026/09/excluding-certain-canadian-alcoholic-beverages-from-importation-into-the-united-states-in-response-to-continued-discrimination-against-the-commerce-of-the-united-states-with-respect-to-alcoholic-bever)
- [Excluding Certain Canadian Products from Importation into the United States in Response to Continued Discrimination Against the Commerce of the United States with Respect to Dairy](https://www.whitehouse.gov/presidential-actions/2026/09/excluding-certain-canadian-products-from-importation-into-the-united-states-in-response-to-continued-discrimination-against-the-commerce-of-the-united-states-with-respect-to-dairy)
- [Excluding Certain Canadian Products from Importation into the United States in Response to Continued Discrimination Against the Commerce of the United States with Respect to Motor Vehicles](https://www.whitehouse.gov/presidential-actions/2026/09/excluding-certain-canadian-products-from-importation-into-the-united-states-in-response-to-continued-discrimination-against-the-commerce-of-the-united-states-with-respect-to-motor-vehicles)

### 114. 3 items; pair scores 90.0%–93.6% (mean 91.8%, 2 qualifying pairs)

Feed: <https://blog.fastcomments.com/rss.xml>

- [(4-14-2021) FastComments Goes Angular](https://blog.fastcomments.com/(4-14-2021)-fastcomments-goes-angular.html)
- [(10-05-2020) Embedding Comments on a VueJS Site With Fastcomments](https://blog.fastcomments.com/(10-05-2020)-embedding-comments-on-a-vuejs-site-with-fastcomments.html)
- [(8-12-2020) FastComments Goes React](https://blog.fastcomments.com/(8-12-2020)-fastcomments-goes-react.html)

### 115. 3 items; pair scores 90.6%–91.6% (mean 91.1%, 2 qualifying pairs)

Feed: <https://essentiallyamsterdam.com/feed/>

- [Essentially Amsterdam – donderdag 1 oktober 2026](https://essentiallyamsterdam.com/newsletter-2026-10-01-nl-4)
- [Essentially Amsterdam – donderdag 1 oktober 2026](https://essentiallyamsterdam.com/newsletter-2026-10-01-nl-3)
- [Essentially Amsterdam – donderdag 1 oktober 2026](https://essentiallyamsterdam.com/newsletter-2026-10-01-nl-2)

### 116. 2 items; pair scores 100.0%–100.0% (mean 100.0%, 1 qualifying pairs)

Feed: <https://abraxian.writeas.com/feed/>

- [I Am Not One of You](https://abraxian.writeas.com/i-am-not-one-of-you-ptc8?pk_campaign=rss-feed)
- [I Am Not One of You](https://abraxian.writeas.com/i-am-not-one-of-you?pk_campaign=rss-feed)

### 117. 2 items; pair scores 100.0%–100.0% (mean 100.0%, 1 qualifying pairs)

Feed: <https://books.uu.se/uup/gateway/plugin/WebFeedGatewayPlugin/rss2>

- [Musiken i Uppsala under stormaktstiden. 2: 1660–1730](https://books.uu.se/uup/catalog/book/30)
- [Musiken i Uppsala under stormaktstiden. 2: 1660–1730](https://books.uu.se/uup/catalog/book/29)

### 118. 2 items; pair scores 100.0%–100.0% (mean 100.0%, 1 qualifying pairs)

Feed: <https://cozystack.io/index.xml>

- [Virtual Machine Disk](https://cozystack.io/docs/v1.5/virtualization/vm-disk)
- [Virtual Machine Disk](https://cozystack.io/docs/v1.6/virtualization/vm-disk)

### 119. 2 items; pair scores 100.0%–100.0% (mean 100.0%, 1 qualifying pairs)

Feed: <https://cozystack.io/index.xml>

- [Managed Kafka Service](https://cozystack.io/docs/v1.5/applications/kafka)
- [Managed Kafka Service](https://cozystack.io/docs/v1.6/applications/kafka)

### 120. 2 items; pair scores 100.0%–100.0% (mean 100.0%, 1 qualifying pairs)

Feed: <https://cozystack.io/index.xml>

- [Managed NATS Service](https://cozystack.io/docs/v1.5/applications/nats)
- [Managed NATS Service](https://cozystack.io/docs/v1.6/applications/nats)

### 121. 2 items; pair scores 100.0%–100.0% (mean 100.0%, 1 qualifying pairs)

Feed: <https://cozystack.io/index.xml>

- [Managed OpenBAO Service](https://cozystack.io/docs/v1.5/applications/openbao)
- [Managed OpenBAO Service](https://cozystack.io/docs/v1.6/applications/openbao)

### 122. 2 items; pair scores 100.0%–100.0% (mean 100.0%, 1 qualifying pairs)

Feed: <https://cozystack.io/index.xml>

- [Ingress-NGINX Controller Reference](https://cozystack.io/docs/v1.5/operations/services/ingress)
- [Ingress-NGINX Controller Reference](https://cozystack.io/docs/v1.6/operations/services/ingress)

### 123. 2 items; pair scores 100.0%–100.0% (mean 100.0%, 1 qualifying pairs)

Feed: <https://cynicaldeveloper.com/feed/podcast>

- [Episode 125 - Agile back to basics](https://cynical.dev/125)
- [Episode 125 - Agile back to basics](https://cynical.dev/125)

### 124. 2 items; pair scores 100.0%–100.0% (mean 100.0%, 1 qualifying pairs)

Feed: <https://cynicaldeveloper.com/feed/podcast>

- [Episode 120 - Effective Developers](http://cynicaldeveloper.com/podcast/120)
- [Episode 120 - Effective Developers](http://cynicaldeveloper.com/podcast/120)

### 125. 2 items; pair scores 100.0%–100.0% (mean 100.0%, 1 qualifying pairs)

Feed: <https://cynicaldeveloper.com/feed/podcast>

- [Episode 87 - Security in IoT](http://cynicaldeveloper.com/podcast/87)
- [Episode 87 - Security in IoT](http://cynicaldeveloper.com/podcast/87)

### 126. 2 items; pair scores 100.0%–100.0% (mean 100.0%, 1 qualifying pairs)

Feed: <https://cynicaldeveloper.com/feed/podcast>

- [Episode 81 - Unit Testing vs Integration Tests](http://cynicaldeveloper.com/podcast/81)
- [Episode 81 - Unit Testing vs Integration Tests](http://cynicaldeveloper.com/podcast/81)

### 127. 2 items; pair scores 100.0%–100.0% (mean 100.0%, 1 qualifying pairs)

Feed: <https://ducttape.libsyn.com/rss>

- [How Inbound Marketing Is Changing in the Age of AI (Rerun)](https://ducttape.libsyn.com/how-inbound-marketing-is-changing-in-the-age-of-ai-rerun)
- [How Inbound Marketing Is Changing in the Age of AI](https://ducttape.libsyn.com/how-inbound-marketing-is-changing-in-the-age-of-ai)

### 128. 2 items; pair scores 100.0%–100.0% (mean 100.0%, 1 qualifying pairs)

Feed: <https://ducttape.libsyn.com/rss>

- [How to Think Strategically About AI Tools](https://ducttape.libsyn.com/how-to-think-strategically-about-ai-tools)
- [How to Think Strategically About AI Tools](https://ducttape.libsyn.com/how-to-think-strategically-about-ai-tools-0)

### 129. 2 items; pair scores 100.0%–100.0% (mean 100.0%, 1 qualifying pairs)

Feed: <https://dylanbeattie.net/rss.xml>

- [Restival Part 6: Who Am I, Revisited](https://dylanbeattie.net/2016/01/07/restival-part-6-who-am-i-revisited.html)
- [Restival Part 6: Who Am I, Revisited](https://dylanbeattie.net/2015/12/08/restival-part-6-who-am-i-revisited.html)

### 130. 2 items; pair scores 100.0%–100.0% (mean 100.0%, 1 qualifying pairs)

Feed: <https://dylanbeattie.net/rss.xml>

- [Restival Part 5: Who Am I?](https://dylanbeattie.net/2016/01/07/restival-part-5-who-am-i.html)
- [Restival Part 5: Who Am I?](https://dylanbeattie.net/2015/12/07/restival-part-5-who-am-i.html)

### 131. 2 items; pair scores 100.0%–100.0% (mean 100.0%, 1 qualifying pairs)

Feed: <https://essentiallyamsterdam.com/feed/>

- [Best Pizza in Amsterdam: 10 Local Picks for 2026](https://essentiallyamsterdam.com/best-pizza-amsterdam-2)
- [Best Pizza in Amsterdam: 10 Local Picks for 2026](https://essentiallyamsterdam.com/best-pizza-amsterdam)

### 132. 2 items; pair scores 100.0%–100.0% (mean 100.0%, 1 qualifying pairs)

Feed: <https://feeds.npr.org/510313/podcast.xml>

- [Advice Line with Jeni Britton of Jeni's Splendid Ice Creams (2025)](https://rss.art19.com/episodes/6dadc048-8cec-44ec-a3cf-ec314a00e41e.mp3?rss_browser=bahjigtdahjvbwugogzfva%3d%3d--d05363d83ce333c74f32188013892b2863ad051c)
- [Advice Line with Jeni Britton of Jeni's Splendid Ice Creams](https://rss.art19.com/episodes/6603d8f1-1a2b-4742-95d6-6e042078e19d.mp3?rss_browser=bahjigtdahjvbwugogzfva%3d%3d--d05363d83ce333c74f32188013892b2863ad051c)

### 133. 2 items; pair scores 100.0%–100.0% (mean 100.0%, 1 qualifying pairs)

Feed: <https://feeds.npr.org/510313/podcast.xml>

- [Advice Line with Norma Kamali of Norma Kamali (November 2024)](https://rss.art19.com/episodes/9f59a14e-e154-402b-8899-3aa2f17b8fd0.mp3?rss_browser=bahjigtdahjvbwugogzfva%3d%3d--d05363d83ce333c74f32188013892b2863ad051c)
- [Advice Line with Norma Kamali of Norma Kamali](https://rss.art19.com/episodes/5fac6958-7fdb-44e3-a956-ae7e1393a883.mp3?rss_browser=bahjigtdahjvbwugogzfva%3d%3d--d05363d83ce333c74f32188013892b2863ad051c)

### 134. 2 items; pair scores 100.0%–100.0% (mean 100.0%, 1 qualifying pairs)

Feed: <https://feeds.npr.org/510313/podcast.xml>

- [Advice Line with Lara Merriken of LÄRABAR (October 2024)](https://rss.art19.com/episodes/3e2ba490-b0e1-4821-867e-1867a7af146b.mp3?rss_browser=bahjigtdahjvbwugogzfva%3d%3d--d05363d83ce333c74f32188013892b2863ad051c)
- [Advice Line with Lara Merriken of LÄRABAR](https://rss.art19.com/episodes/da8ecafa-00c3-42cc-829e-a1ad8e0908e5.mp3?rss_browser=bahjigtdahjvbwugogzfva%3d%3d--d05363d83ce333c74f32188013892b2863ad051c)

### 135. 2 items; pair scores 100.0%–100.0% (mean 100.0%, 1 qualifying pairs)

Feed: <https://feeds.npr.org/510313/podcast.xml>

- [Advice Line with Brett Schulman of CAVA (July 2024)](https://rss.art19.com/episodes/742f71dd-66b5-474b-88b8-edae6f5c5ece.mp3?rss_browser=bahjigtdahjvbwugogzfva%3d%3d--d05363d83ce333c74f32188013892b2863ad051c)
- [Advice Line with Brett Schulman of CAVA](https://rss.art19.com/episodes/8b2fd9c0-0284-47de-b678-834b5804f83f.mp3?rss_browser=bahjigtdahjvbwugogzfva%3d%3d--d05363d83ce333c74f32188013892b2863ad051c)

### 136. 2 items; pair scores 100.0%–100.0% (mean 100.0%, 1 qualifying pairs)

Feed: <https://feeds.npr.org/510313/podcast.xml>

- [Designing shoes for women's feet with Wes and Allyson Felix of Saysh (2023)](https://rss.art19.com/episodes/77e1108f-83fa-40d7-908b-ca46c396a88a.mp3?rss_browser=bahjigtdahjvbwugogzfva%3d%3d--d05363d83ce333c74f32188013892b2863ad051c)
- [HIBT Lab! Saysh: Wes and Allyson Felix](https://rss.art19.com/episodes/82e74cf3-dcb9-412e-a467-e8f1427e5687.mp3?rss_browser=bahjigtdahjvbwugogzfva%3d%3d--d05363d83ce333c74f32188013892b2863ad051c)

### 137. 2 items; pair scores 100.0%–100.0% (mean 100.0%, 1 qualifying pairs)

Feed: <https://feeds.npr.org/510333/podcast.xml>

- [Do Not Pass Go (2022)](https://www.npr.org/2022/12/16/1143650089/do-not-pass-go-2022)
- [Do Not Pass Go](https://www.npr.org/2022/06/29/1108728257/do-not-pass-go)

### 138. 2 items; pair scores 100.0%–100.0% (mean 100.0%, 1 qualifying pairs)

Feed: <https://feeds.simplecast.com/dLRotFGk>

- [Don't Fear AI Taking the Coding Jobs (Fixed Audio)](http://www.developertea.com)
- [Don't Fear AI Taking the Coding Jobs](http://www.developertea.com)

### 139. 2 items; pair scores 100.0%–100.0% (mean 100.0%, 1 qualifying pairs)

Feed: <https://laist.com/index.rss>

- [LA was supposed to have a no-cost Olympics in 2028. The city has already spent $27 million](https://laist.com/news/politics/la-olympics-spending-27-million)
- [LA was supposed to have a no-cost Olympics in 2028. The city has already spent $27 million](https://laist.com/news/politics/la-olympics-spending-27-million)

### 140. 2 items; pair scores 100.0%–100.0% (mean 100.0%, 1 qualifying pairs)

Feed: <https://laist.com/index.rss>

- [LA voters could give huge power to a city role that hasn't really existed for 20 years](https://laist.com/news/politics/charter-amendment-la-public-works-director-mystery)
- [LA voters could give huge power to a city role that hasn't really existed for 20 years](https://laist.com/news/politics/charter-amendment-la-public-works-director-mystery)

### 141. 2 items; pair scores 100.0%–100.0% (mean 100.0%, 1 qualifying pairs)

Feed: <https://laist.com/index.rss>

- [Find world class Black art in these LA County libraries and community centers](https://laist.com/news/los-angeles-activities/where-to-see-black-art-la-county-libraries-community-centers)
- [Find world class Black art in these LA County libraries and community centers](https://laist.com/news/los-angeles-activities/where-to-see-black-art-la-county-libraries-community-centers)

### 142. 2 items; pair scores 100.0%–100.0% (mean 100.0%, 1 qualifying pairs)

Feed: <https://libriscv.no/blog/rss.xml>

- [Automatia Update: All Aboard](https://libriscv.no/blog/all-aboard)
- [Automatia Update: All Aboard!](https://libriscv.no/blog/all-aboard)

### 143. 2 items; pair scores 100.0%–100.0% (mean 100.0%, 1 qualifying pairs)

Feed: <https://micro.mjanssen.nl/feed.xml>

- [👞SOREL Caribou Boots: Great Product, Disappointing Online Shop](https://micro.mjanssen.nl/2026/07/15/200303.html)
- [👞SOREL Caribou Boots: Great Product, Disappointing Online Shop](https://micro.mjanssen.nl/2026/07/15/sorel-caribou-boots-great-product.html)

### 144. 2 items; pair scores 100.0%–100.0% (mean 100.0%, 1 qualifying pairs)

Feed: <https://momentsingraphics.de/RSS.xml>

- [Versatile Geometric Flow Visualization by Controllable Shape and Volumetric Appearance](http://momentsingraphics.de/stag2022.html)
- [Moment-Based Opacity Optimization](http://momentsingraphics.de/egpgv2020.html)

### 145. 2 items; pair scores 100.0%–100.0% (mean 100.0%, 1 qualifying pairs)

Feed: <https://nathannaveen.dev/index.xml>

- [Leetcode 1427](https://nathannaveen.dev/posts/leetcode-1427)
- [Leetcode 163](https://nathannaveen.dev/posts/leetcode-163)

### 146. 2 items; pair scores 100.0%–100.0% (mean 100.0%, 1 qualifying pairs)

Feed: <https://open-web-advocacy.org/feed.xml>

- [London Web Standards OWA Meetup](https://open-web-advocacy.org/ja/blog/london-web-standards-owa-meetup)
- [London Web Standards OWA Meetup](https://open-web-advocacy.org/es/blog/london-web-standards-owa-meetup)

### 147. 2 items; pair scores 100.0%–100.0% (mean 100.0%, 1 qualifying pairs)

Feed: <https://open-web-advocacy.org/feed.xml>

- [Bruce Lawson Interviews OWA: For A Better Web](https://open-web-advocacy.org/blog/bruce-lawson-interviews-owa--for-a-better-web)
- [Bruce Lawson Interviews OWA: For A Better Web](https://open-web-advocacy.org/es/blog/bruce-lawson-interviews-owa--for-a-better-web)

### 148. 2 items; pair scores 100.0%–100.0% (mean 100.0%, 1 qualifying pairs)

Feed: <https://open-web-advocacy.org/feed.xml>

- [28% Faster: The Blink Prototype That Shows Why Apple's iOS Browser Engine Ban Must End](https://open-web-advocacy.org/blog/28-percent-faster--the-blink-prototype-that-shows-why-apples-ios-browser-engine-ban-must-end)
- [28% Faster: The Blink Prototype That Shows Why Apple's iOS Browser Engine Ban Must End](https://open-web-advocacy.org/es/blog/28-percent-faster--the-blink-prototype-that-shows-why-apples-ios-browser-engine-ban-must-end)

### 149. 2 items; pair scores 100.0%–100.0% (mean 100.0%, 1 qualifying pairs)

Feed: <https://open-web-advocacy.org/feed.xml>

- [How Apple’s Key Tactic Could Prevent Japan’s Smartphone Act from Improving Browser Competition](https://open-web-advocacy.org/blog/how_apples_key_tactic_could_prevent_japans_smartphone_act_from_improving_browser_competition)
- [How Apple’s Key Tactic Could Prevent Japan’s Smartphone Act from Improving Browser Competition](https://open-web-advocacy.org/es/blog/how_apples_key_tactic_could_prevent_japans_smartphone_act_from_improving_browser_competition)

### 150. 2 items; pair scores 100.0%–100.0% (mean 100.0%, 1 qualifying pairs)

Feed: <https://oscarstories.com/rss.xml>

- [Alice's Abenteuer im Wunderland - Das Hummerballet](https://oscarstories.com/blog/de/alice-in-wonderland-chapter-10-de)
- [Alice's Abenteuer im Wunderland - Das Hummerballet](https://oscarstories.com/blog/de/alice-in-wonderland-chapter-10)

### 151. 2 items; pair scores 100.0%–100.0% (mean 100.0%, 1 qualifying pairs)

Feed: <https://oscarstories.com/rss.xml>

- [Alice's Abenteuer im Wunderland - Alice ist die Klügste](https://oscarstories.com/blog/de/alice-in-wonderland-chapter-11-de)
- [Alice's Abenteuer im Wunderland - Alice ist die Klügste](https://oscarstories.com/blog/de/alice-in-wonderland-chapter-11)

### 152. 2 items; pair scores 100.0%–100.0% (mean 100.0%, 1 qualifying pairs)

Feed: <https://oscarstories.com/rss.xml>

- [Alice's Abenteuer im Wunderland - Die tolle Theegesellschaft](https://oscarstories.com/blog/de/alice-in-wonderland-chapter-7-de)
- [Alice's Abenteuer im Wunderland - Die tolle Theegesellschaft](https://oscarstories.com/blog/de/alice-in-wonderland-chapter-7)

### 153. 2 items; pair scores 100.0%–100.0% (mean 100.0%, 1 qualifying pairs)

Feed: <https://oscarstories.com/rss.xml>

- [Alice's Abenteuer im Wunderland - Das Croquetfeld der Königin](https://oscarstories.com/blog/de/alice-in-wonderland-chapter-8-de)
- [Alice's Abenteuer im Wunderland - Das Croquetfeld der Königin](https://oscarstories.com/blog/de/alice-in-wonderland-chapter-8)

### 154. 2 items; pair scores 100.0%–100.0% (mean 100.0%, 1 qualifying pairs)

Feed: <https://oscarstories.com/rss.xml>

- [Alice's Abenteuer im Wunderland - Die Geschichte der falschen Schildkröte](https://oscarstories.com/blog/de/alice-in-wonderland-chapter-9-de)
- [Alice's Abenteuer im Wunderland - Die Geschichte der falschen Schildkröte](https://oscarstories.com/blog/de/alice-in-wonderland-chapter-9)

### 155. 2 items; pair scores 100.0%–100.0% (mean 100.0%, 1 qualifying pairs)

Feed: <https://oscarstories.com/rss.xml>

- [Alice's Abenteuer im Wunderland - Caucus-Rennen und was daraus wird](https://oscarstories.com/blog/de/alice-in-wonderland-chapter-3-de)
- [Alice's Abenteuer im Wunderland - Caucus-Rennen und was daraus wird](https://oscarstories.com/blog/de/alice-in-wonderland-chapter-3)

### 156. 2 items; pair scores 100.0%–100.0% (mean 100.0%, 1 qualifying pairs)

Feed: <https://oscarstories.com/rss.xml>

- [Alice's Abenteuer im Wunderland - Die Wohnung des Kaninchens](https://oscarstories.com/blog/de/alice-in-wonderland-chapter-4-de)
- [Alice's Abenteuer im Wunderland - Die Wohnung des Kaninchens](https://oscarstories.com/blog/de/alice-in-wonderland-chapter-4)

### 157. 2 items; pair scores 100.0%–100.0% (mean 100.0%, 1 qualifying pairs)

Feed: <https://oscarstories.com/rss.xml>

- [Alice's Abenteuer im Wunderland - Guter Rath von einer Raupe](https://oscarstories.com/blog/de/alice-in-wonderland-chapter-5-de)
- [Alice's Abenteuer im Wunderland - Guter Rath von einer Raupe](https://oscarstories.com/blog/de/alice-in-wonderland-chapter-5)

### 158. 2 items; pair scores 100.0%–100.0% (mean 100.0%, 1 qualifying pairs)

Feed: <https://oscarstories.com/rss.xml>

- [Alice's Abenteuer im Wunderland - Ferkel und Pfeffer](https://oscarstories.com/blog/de/alice-in-wonderland-chapter-6-de)
- [Alice's Abenteuer im Wunderland - Ferkel und Pfeffer](https://oscarstories.com/blog/de/alice-in-wonderland-chapter-6)

### 159. 2 items; pair scores 100.0%–100.0% (mean 100.0%, 1 qualifying pairs)

Feed: <https://oscarstories.com/rss.xml>

- [Alice's Abenteuer im Wunderland - Der Thränenpfuhl](https://oscarstories.com/blog/de/alice-in-wonderland-chapter-2-de)
- [Alice's Abenteuer im Wunderland - Der Thränenpfuhl](https://oscarstories.com/blog/de/alice-in-wonderland-chapter-2)

### 160. 2 items; pair scores 100.0%–100.0% (mean 100.0%, 1 qualifying pairs)

Feed: <https://oscarstories.com/rss.xml>

- [Pourquoi les histoires pour enfants avec des images sont plus qu'un simple amusement](https://oscarstories.com/blog/fr/_children_stories_with_pictures)
- [Pourquoi les histoires pour enfants avec des images sont plus qu'un simple amusement](https://oscarstories.com/blog/fr/it_children_stories_with_pictures)

### 161. 2 items; pair scores 100.0%–100.0% (mean 100.0%, 1 qualifying pairs)

Feed: <https://people.kernel.org/read/feed/>

- [Notes about Netiquette](https://people.kernel.org/tglx/notes-about-netiquette-qw89)
- [Notes about Netiquette](https://people.kernel.org/tglx/notes-about-netiquette)

### 162. 2 items; pair scores 100.0%–100.0% (mean 100.0%, 1 qualifying pairs)

Feed: <https://podcasts.files.bbci.co.uk/p002w557.rss>

- [Ignaz Semmelweiss: The hand washer](http://www.bbc.co.uk/programmes/w3csz9dc)
- [Ignaz Semmelweiss: The hand washer](http://www.bbc.co.uk/programmes/w3csy6cv)

### 163. 2 items; pair scores 100.0%–100.0% (mean 100.0%, 1 qualifying pairs)

Feed: <https://reaper.is/rss.xml>

- [Writing cleaner state in React and React Native](https://reaper.is/writing/hello-world.html)
- [Writing cleaner state in React and React Native](https://reaper.is/writing/writing-cleaner-state-in-react.html)

### 164. 2 items; pair scores 100.0%–100.0% (mean 100.0%, 1 qualifying pairs)

Feed: <https://rebecca-powell.com/rss.xml>

- [AstroPaper 3.0](https://rebecca-powell.com/posts/astro-paper-v3)
- [AstroPaper 3.0](https://rebecca-powell.com/posts/astro-paper-v3)

### 165. 2 items; pair scores 100.0%–100.0% (mean 100.0%, 1 qualifying pairs)

Feed: <https://rebecca-powell.com/rss.xml>

- [Tailwind Typography Plugin](https://rebecca-powell.com/posts/tailwind-typography)
- [Tailwind Typography Plugin](https://rebecca-powell.com/posts/tailwind-typography)

### 166. 2 items; pair scores 100.0%–100.0% (mean 100.0%, 1 qualifying pairs)

Feed: <https://rebecca-powell.com/rss.xml>

- [How Do I Develop My Terminal Portfolio Website with React](https://rebecca-powell.com/posts/how-do-i-develop-my-terminal-portfolio-website-with-react)
- [How Do I Develop My Terminal Portfolio Website with React](https://rebecca-powell.com/posts/how-do-i-develop-my-terminal-portfolio-website-with-react)

### 167. 2 items; pair scores 100.0%–100.0% (mean 100.0%, 1 qualifying pairs)

Feed: <https://rebecca-powell.com/rss.xml>

- [How Do I Develop My Portfolio Website & Blog](https://rebecca-powell.com/posts/how-do-i-develop-my-portfolio-and-blog)
- [How Do I Develop My Portfolio Website & Blog](https://rebecca-powell.com/posts/how-do-i-develop-my-portfolio-and-blog)

### 168. 2 items; pair scores 100.0%–100.0% (mean 100.0%, 1 qualifying pairs)

Feed: <https://rebecca-powell.com/rss.xml>

- [AstroPaper 2.0](https://rebecca-powell.com/posts/astro-paper-2)
- [AstroPaper 2.0](https://rebecca-powell.com/posts/astro-paper-2)

### 169. 2 items; pair scores 100.0%–100.0% (mean 100.0%, 1 qualifying pairs)

Feed: <https://rebecca-powell.com/rss.xml>

- [AstroPaper 4.0](https://rebecca-powell.com/posts/astro-paper-v4)
- [AstroPaper 4.0](https://rebecca-powell.com/posts/astro-paper-v4)

### 170. 2 items; pair scores 100.0%–100.0% (mean 100.0%, 1 qualifying pairs)

Feed: <https://robrace.dev/blog/rss.xml>

- [How to Build a Twitter Clone with Rails and Hotwire](https://robrace.dev/blog/build-a-twitter-clone-with-rails-hotwire)
- [How to Build a Twitter Clone with Rails and Hotwire](https://robrace.dev/blog/rails-hotwire)

### 171. 2 items; pair scores 100.0%–100.0% (mean 100.0%, 1 qualifying pairs)

Feed: <https://scholar.dominican.edu/recent.rss>

- [Words Taking Root: The Psychological Effects of Eco-Poetry on Eco-Anxiety and Connectedness to Nature](https://scholar.dominican.edu/psychology-senior-theses/19)
- [Words Taking Root: The Psychological Effects of Eco-Poetry on Eco-Anxiety and Connectedness to Nature](https://scholar.dominican.edu/psychology-student-research-posters/5)

### 172. 2 items; pair scores 100.0%–100.0% (mean 100.0%, 1 qualifying pairs)

Feed: <https://scholarsarchive.byu.edu/recent.rss>

- [Socially Advantaged? How Social Affiliations Influence Access to Valuable Service Professional Transactions](https://scholarsarchive.byu.edu/facpub/9881)
- [Socially Advantaged? How Social Affiliations Influence Access to Valuable Service Professional Transactions](https://scholarsarchive.byu.edu/facpub/9872)

### 173. 2 items; pair scores 100.0%–100.0% (mean 100.0%, 1 qualifying pairs)

Feed: <https://scholarsarchive.byu.edu/recent.rss>

- [Connected, but Qualified? Social Affiliations, Human Capital, and Service Professional Performance](https://scholarsarchive.byu.edu/facpub/9878)
- [Connected, but Qualified? Social Affiliations, Human Capital, and Service Professional Performance](https://scholarsarchive.byu.edu/facpub/9873)

### 174. 2 items; pair scores 100.0%–100.0% (mean 100.0%, 1 qualifying pairs)

Feed: <https://spaceinformer.com/feed/>

- [Best Tools for Eclipse Planning That Work](https://spaceinformer.com/best-tools-for-eclipse-planning)
- [Best Tools for Eclipse Planning That Work](https://spaceinformer.com/best-tools-for-eclipse-planning)

### 175. 2 items; pair scores 100.0%–100.0% (mean 100.0%, 1 qualifying pairs)

Feed: <https://sports.yahoo.com/rss/>

- [Rain expected for Tennessee football vs Auburn, see latest weather forecast](https://sports.yahoo.com/articles/rain-expected-tennessee-football-vs-143820789.html)
- [Rain expected for Tennessee football vs Auburn, see latest weather forecast](https://sports.yahoo.com/articles/rain-expected-tennessee-football-vs-143820194.html)

### 176. 2 items; pair scores 100.0%–100.0% (mean 100.0%, 1 qualifying pairs)

Feed: <https://startupjunkie.libsyn.com/rss>

- [142: Chatting with a Champion - Tom Chapman (Rebroadcast)](https://share.transistor.fm/s/581bf76f)
- [142: Chatting with a Champion - Tom Chapman](https://share.transistor.fm/s/d4159f33)

### 177. 2 items; pair scores 100.0%–100.0% (mean 100.0%, 1 qualifying pairs)

Feed: <https://techcrawlr.com/feed/>

- [Why Longer Casino Games Still Have a Place Online](https://techcrawlr.com/why-longer-casino-games-still-have-a-place-online-2)
- [Why Longer Casino Games Still Have a Place Online](https://techcrawlr.com/why-longer-casino-games-still-have-a-place-online)

### 178. 2 items; pair scores 100.0%–100.0% (mean 100.0%, 1 qualifying pairs)

Feed: <https://unitary.foundation/posts/feed.xml>

- [unitaryHACK 2022: The Unitary Fund hackathon supporting quantum open source projects returns from June 3rd, 2022](https://unitary.foundation/posts/2022unitaryhack)
- [Celebrating quantum open-source software contributors: Announcing the 2021 Wittek prize winner with QOSF](https://unitary.foundation/posts/2022_wittek_prize)

### 179. 2 items; pair scores 100.0%–100.0% (mean 100.0%, 1 qualifying pairs)

Feed: <https://www.buchinger-wilhelmi.com/feed/>

- [Villa Mariposa](https://www.buchinger-wilhelmi.com/villa-mariposa)
- [Villa Mariposa](https://www.buchinger-wilhelmi.com/neue-unterkunft-in-marbella)

### 180. 2 items; pair scores 100.0%–100.0% (mean 100.0%, 1 qualifying pairs)

Feed: <https://www.efinancialcareers.com/feed/syndication/rss.xml>

- [Goldman Sachs' most junior bankers don't want to speak to you unless it's in person](https://www.efinancialcareers.com/news/goldman-sachs-most-junior-bankers-don-t-want-to-speak-to-you-unless-it-s-in-person)
- [Goldman Sachs' most junior bankers don't want to speak to you unless it's in person](https://www.efinancialcareers.com/news/overwrite-goldman-sachs-most-junior-bankers-don-t-want-to-speak-to-you-unless-it-s-in-person)

### 181. 2 items; pair scores 100.0%–100.0% (mean 100.0%, 1 qualifying pairs)

Feed: <https://www.efinancialcareers.com/feed/syndication/rss.xml>

- [Evercore's London pay shot up. But only for a select group](https://www.efinancialcareers.com/news/evercore-london-pay)
- [Evercore's London pay shot up. But only for a select group](https://www.efinancialcareers.com/news/2023/09/evercore-london-pay)

### 182. 2 items; pair scores 100.0%–100.0% (mean 100.0%, 1 qualifying pairs)

Feed: <https://www.efinancialcareers.com/feed/syndication/rss.xml>

- [Jane Street's UK pay: $1.9m per head, $55m if you're a partner](https://www.efinancialcareers.com/news/jane-street-pay)
- [Jane Street's UK pay: $1.9m per head, $55m if you're a partner](https://www.efinancialcareers.com/news/2023/07/jane-street-pay)

### 183. 2 items; pair scores 100.0%–100.0% (mean 100.0%, 1 qualifying pairs)

Feed: <https://www.efinancialcareers.com/feed/syndication/rss.xml>

- ["The images and feelings of 9/11 remain branded on my mind."](https://www.efinancialcareers.com/news/911-ten-years-after-reflections-of-a-survivor-2)
- ["The images and feelings of 9/11 remain branded on my mind."](https://www.efinancialcareers.com/news/2015/09/911-ten-years-after-reflections-of-a-survivor-2)

### 184. 2 items; pair scores 100.0%–100.0% (mean 100.0%, 1 qualifying pairs)

Feed: <https://www.efinancialcareers.com/feed/syndication/rss.xml>

- [How to get a job in venture capital](https://www.efinancialcareers.com/news/how-to-get-a-job-in-venture-capital)
- [How to get a job in venture capital](https://www.efinancialcareers.com/news/finance/how-to-get-a-job-in-venture-capital)

### 185. 2 items; pair scores 100.0%–100.0% (mean 100.0%, 1 qualifying pairs)

Feed: <https://www.grizzlypeaksoftware.com/feed>

- [Debugging Memory Leaks in Node.js](https://www.grizzlypeaksoftware.com/library/debugging-memory-leaks-in-nodejs-ktuxxx2y)
- [Debugging Memory Leaks in Node.js](https://www.grizzlypeaksoftware.com/library/debugging-memory-leaks-in-nodejs-ttrup66x)

### 186. 2 items; pair scores 100.0%–100.0% (mean 100.0%, 1 qualifying pairs)

Feed: <https://www.grizzlypeaksoftware.com/feed>

- [Node.js Performance Optimization Techniques](https://www.grizzlypeaksoftware.com/library/nodejs-performance-optimization-techniques-auudmac4)
- [Node.js Performance Optimization Techniques](https://www.grizzlypeaksoftware.com/library/nodejs-performance-optimization-techniques-wroxi07d)

### 187. 2 items; pair scores 100.0%–100.0% (mean 100.0%, 1 qualifying pairs)

Feed: <https://www.grizzlypeaksoftware.com/feed>

- [Node.js Logging Best Practices with Winston](https://www.grizzlypeaksoftware.com/library/nodejs-logging-best-practices-with-winston-9wq8vk5q)
- [Node.js Logging Best Practices with Winston](https://www.grizzlypeaksoftware.com/library/nodejs-logging-best-practices-with-winston-nht2wz43)

### 188. 2 items; pair scores 100.0%–100.0% (mean 100.0%, 1 qualifying pairs)

Feed: <https://www.grizzlypeaksoftware.com/feed>

- [Rate Limiting Express.js APIs](https://www.grizzlypeaksoftware.com/library/rate-limiting-expressjs-apis-vdh1134g)
- [Rate Limiting Express.js APIs](https://www.grizzlypeaksoftware.com/library/rate-limiting-expressjs-apis-ap7su3u5)

### 189. 2 items; pair scores 100.0%–100.0% (mean 100.0%, 1 qualifying pairs)

Feed: <https://www.grizzlypeaksoftware.com/feed>

- [JWT Authentication in Express.js Applications](https://www.grizzlypeaksoftware.com/library/jwt-authentication-in-expressjs-applications-km42ybfn)
- [JWT Authentication in Express.js Applications](https://www.grizzlypeaksoftware.com/library/jwt-authentication-in-expressjs-applications-uado81bb)

### 190. 2 items; pair scores 100.0%–100.0% (mean 100.0%, 1 qualifying pairs)

Feed: <https://www.grizzlypeaksoftware.com/feed>

- [PowerShell Automation for Development Workflows](https://www.grizzlypeaksoftware.com/library/powershell-automation-for-development-workflows-r3zzbczn)
- [PowerShell Automation for Development Workflows](https://www.grizzlypeaksoftware.com/library/powershell-automation-for-development-workflows-cqq97hx1)

### 191. 2 items; pair scores 100.0%–100.0% (mean 100.0%, 1 qualifying pairs)

Feed: <https://www.grizzlypeaksoftware.com/feed>

- [Shell Scripting Best Practices for Developers](https://www.grizzlypeaksoftware.com/library/shell-scripting-best-practices-for-developers-rw0mfp6e)
- [Shell Scripting Best Practices for Developers](https://www.grizzlypeaksoftware.com/library/shell-scripting-best-practices-for-developers-4wj2nzcb)

### 192. 2 items; pair scores 100.0%–100.0% (mean 100.0%, 1 qualifying pairs)

Feed: <https://www.grizzlypeaksoftware.com/feed>

- [Building Interactive CLIs with Node.js and Inquirer](https://www.grizzlypeaksoftware.com/library/building-interactive-clis-with-nodejs-and-inquirer-ibi4wrjt)
- [Building Interactive CLIs with Node.js and Inquirer](https://www.grizzlypeaksoftware.com/library/building-interactive-clis-with-nodejs-and-inquirer-zda12oy1)

### 193. 2 items; pair scores 100.0%–100.0% (mean 100.0%, 1 qualifying pairs)

Feed: <https://www.grizzlypeaksoftware.com/feed>

- [Docker Secrets and Configuration Management](https://www.grizzlypeaksoftware.com/library/docker-secrets-and-configuration-management-gvp61wei)
- [Docker Secrets and Configuration Management](https://www.grizzlypeaksoftware.com/library/docker-secrets-and-configuration-management-j7gdy4r5)

### 194. 2 items; pair scores 100.0%–100.0% (mean 100.0%, 1 qualifying pairs)

Feed: <https://www.grizzlypeaksoftware.com/feed>

- [Multi-Architecture Docker Images](https://www.grizzlypeaksoftware.com/library/multi-architecture-docker-images-unvnilwd)
- [Multi-Architecture Docker Images](https://www.grizzlypeaksoftware.com/library/multi-architecture-docker-images-tivwhacu)

### 195. 2 items; pair scores 100.0%–100.0% (mean 100.0%, 1 qualifying pairs)

Feed: <https://www.infoworld.com/feed/>

- [Starfleet will fly Postgres AI apps from prototype to production, says pgEdge](https://www.infoworld.com/article/4226623/starfleet-will-fly-postgres-ai-apps-from-prototype-to-production-says-pgedge.html)
- [Starfleet will fly Postgres AI apps from prototype to production, says pgEdge](https://www.infoworld.com/article/100064509/starfleet-will-fly-postgres-ai-apps-from-prototype-to-production-says-pgedge-2.html)

### 196. 2 items; pair scores 100.0%–100.0% (mean 100.0%, 1 qualifying pairs)

Feed: <https://www.kelly.senate.gov/feed/>

- [Kelly Secures Key Arizona and National Security Priorities in Final Passage of Annual Defense Bill](https://www.kelly.senate.gov/kelly-secures-key-arizona-and-national-security-priorities-in-final-passage-of-annual-defense-bill-2)
- [Kelly Secures Key Arizona and National Security Priorities in Final Passage of Annual Defense Bill](https://www.kelly.senate.gov/kelly-secures-key-arizona-and-national-security-priorities-in-final-passage-of-annual-defense-bill)

### 197. 2 items; pair scores 100.0%–100.0% (mean 100.0%, 1 qualifying pairs)

Feed: <https://zenmemes.com/?format=rss>

- [Anomaly Detection Through Explanations | Leilani H. Gilpin](http://localhost/threads/thread-20260505213912-0e2b42ea)
- [Anomaly Detection Through Explanations | Leilani H. Gilpin](http://localhost/threads/thread-20260505213920-7dd7598d)

### 198. 2 items; pair scores 99.8%–99.8% (mean 99.8%, 1 qualifying pairs)

Feed: <https://startupreporter.in/feed/>

- [When The Government Wants A Seat At The AI Table: OpenAI’s Reported Equity Talks Spark A New Debate](https://startupreporter.in/when-the-government-wants-a-seat-at-the-ai-table-openais-reported-equity-talks-spark-a-new-debate-2)
- [When The Government Wants A Seat At The AI Table: OpenAI’s Reported Equity Talks Spark A New Debate](https://startupreporter.in/when-the-government-wants-a-seat-at-the-ai-table-openais-reported-equity-talks-spark-a-new-debate)

### 199. 2 items; pair scores 99.7%–99.7% (mean 99.7%, 1 qualifying pairs)

Feed: <https://theamphour.com/feed/podcast/>

- [Chairs, Sparks and Devices - Optional Olent Obreption](https://theamphour.com/191-chairs-sparks-and-devices-optional-olent-obreption)
- [Analog Devices, Design Spark - Unusual Usenet Usurpation](https://theamphour.com/the-amp-hour-49-unusual-usenet-ursurpation)

### 200. 2 items; pair scores 99.7%–99.7% (mean 99.7%, 1 qualifying pairs)

Feed: <https://plaindrops.de/de/rss.xml>

- [Mein Tastaturlayout Edwin](https://plaindrops.de/de/blog/2023/edwin)
- [Mein Tastaturlayout](https://plaindrops.de/de/blog/2022/moonlanderlayout)

### 201. 2 items; pair scores 99.6%–99.6% (mean 99.6%, 1 qualifying pairs)

Feed: <https://blog.armbian.com/rss/>

- [Github Highlights](https://blog.armbian.com/github-highlights-40)
- [Github Highlights](https://blog.armbian.com/github-highlights-38)

### 202. 2 items; pair scores 99.6%–99.6% (mean 99.6%, 1 qualifying pairs)

Feed: <https://www.armbian.com/feed/>

- [Github Highlights](https://blog.armbian.com/github-highlights-40)
- [Github Highlights](https://blog.armbian.com/github-highlights-38)

### 203. 2 items; pair scores 99.6%–99.6% (mean 99.6%, 1 qualifying pairs)

Feed: <https://rss.art19.com/tim-ferriss-show>

- [Ep 27: Kevin Kelly (Part 3) - WIRED Co-Founder, Polymath, Most Interesting Man In The World?](https://rss.art19.com/episodes/29d135c2-52c2-4253-a7a1-4c084d6893d3.mp3?rss_browser=bahjigtdahjvbwugogzfva%3d%3d--d05363d83ce333c74f32188013892b2863ad051c)
- [Ep 26: Kevin Kelly (Part 2) - WIRED Co-Founder, Polymath, Most Interesting Man In The World?](https://rss.art19.com/episodes/e77c8948-ef94-48a2-ab97-cb3c114ebb28.mp3?rss_browser=bahjigtdahjvbwugogzfva%3d%3d--d05363d83ce333c74f32188013892b2863ad051c)

### 204. 2 items; pair scores 99.6%–99.6% (mean 99.6%, 1 qualifying pairs)

Feed: <https://oscarstories.com/rss.xml>

- [Créer des histoires du soir : Oscar Stories vs. ChatGPT](https://oscarstories.com/blog/fr/fr_oscar-stories-chatgpt)
- [Créer des histoires du soir : Oscar Stories vs. ChatGPT](https://oscarstories.com/blog/fr/oscar-stories-vs-chatgpt-pour-histoires-du-soir)

### 205. 2 items; pair scores 99.6%–99.6% (mean 99.6%, 1 qualifying pairs)

Feed: <https://oscarstories.com/rss.xml>

- [Bedtime Stories for Toddlers](https://oscarstories.com/blog/en/bedtime-stories-for-toddlers)
- [Bedtime Stories for Kids](https://oscarstories.com/blog/en/bedtime-stories-for-kids)

### 206. 2 items; pair scores 99.6%–99.6% (mean 99.6%, 1 qualifying pairs)

Feed: <https://site.sebasmonia.com/feed.xml>

- [It finally happened: caught fish](https://site.sebasmonia.com/posts/2026-06-22-it-finally-happened--caught-fish.html)
- [It finally happened: caught fish](https://site.sebasmonia.com/posts/2026-06-22-it-finally-happened--caught-fish.html)

### 207. 2 items; pair scores 99.5%–99.5% (mean 99.5%, 1 qualifying pairs)

Feed: <https://alexalejandre.com//index.xml>

- [Interview With Steve Klabnik](https://alexalejandre.com/interviews/interview-with-steve-klabnik)
- [Interview With Steve Klabnik](https://alexalejandre.com/programming/steve-klabnik-interview)

### 208. 2 items; pair scores 99.4%–99.4% (mean 99.4%, 1 qualifying pairs)

Feed: <https://newsx.com/feed/>

- [As Healthcare Affordability Returns to Parliament, Jose Peter of Arogya Finance on the Financing Gap Beyond Insurance](https://www.newsx.com/business/as-healthcare-affordability-returns-to-parliament-jose-peter-of-arogya-finance-on-the-financing-gap-beyond-insurance-2-278316)
- [As Healthcare Affordability Returns to Parliament, Jose Peter of Arogya Finance on the Financing Gap Beyond Insurance](https://www.newsx.com/business-2/as-healthcare-affordability-returns-to-parliament-jose-peter-of-arogya-finance-on-the-financing-gap-beyond-insurance-278312)

### 209. 2 items; pair scores 99.3%–99.3% (mean 99.3%, 1 qualifying pairs)

Feed: <https://www.protectingtaxpayers.org/feed/>

- [TPA Slams Proposal Targeting Private Equity](https://www.protectingtaxpayers.org/congress/tpa-slams-proposal-targeting-private-equity)
- [Watchdog Slams Proposal Targeting Private Equity](https://www.protectingtaxpayers.org/press/watchdog-slams-proposal-targeting-private-equity)

### 210. 2 items; pair scores 99.3%–99.3% (mean 99.3%, 1 qualifying pairs)

Feed: <https://rss.art19.com/tim-ferriss-show>

- [Ep 53: Ed Cooke (Part 2), Grandmaster of Memory, on Mental Performance, Imagination, and Productive Mischief](https://rss.art19.com/episodes/502155eb-989c-4885-90c9-97b1bc59ab2e.mp3?rss_browser=bahjigtdahjvbwugogzfva%3d%3d--d05363d83ce333c74f32188013892b2863ad051c)
- [Ep 52: Ed Cooke, Grandmaster of Memory, on Mental Performance, Imagination, and Productive Mischief](https://rss.art19.com/episodes/b65c885f-03a8-439f-91b4-b2b5bb8b32c0.mp3?rss_browser=bahjigtdahjvbwugogzfva%3d%3d--d05363d83ce333c74f32188013892b2863ad051c)

### 211. 2 items; pair scores 99.2%–99.2% (mean 99.2%, 1 qualifying pairs)

Feed: <https://posthog.com/rss.xml>

- [Using Gatsby and Puppeteer to create dynamic Open Graph images](https://posthog.com/blog/dynamic-open-graph-images)
- [Using Gatsby and Puppeteer to create dynamic Open Graph images](https://posthog.com/library/design/dynamic-open-graph-images)

### 212. 2 items; pair scores 99.0%–99.0% (mean 99.0%, 1 qualifying pairs)

Feed: <https://www.protectingtaxpayers.org/feed/>

- [As National Debt Tops $40 Trillion, TPA Unveils $10 Trillion Spending Cut Blueprint](https://www.protectingtaxpayers.org/spending/deficit/as-national-debt-tops-40-trillion-tpa-unveils-10-trillion-spending-cut-blueprint)
- [As National Debt Tops $40 Trillion, Watchdog Unveils $10 Trillion Spending Cut Blueprint](https://www.protectingtaxpayers.org/press/as-national-debt-tops-40-trillion-watchdog-unveils-10-trillion-spending-cut-blueprint)

### 213. 2 items; pair scores 99.0%–99.0% (mean 99.0%, 1 qualifying pairs)

Feed: <https://www.postnewsgroup.com/feed/>

- [Event Preview: ‘Fashion v. Fascism’ – A Runway Revolution Is Coming to the Bay Area](https://www.postnewsgroup.com/event-preview-fashion-v-fascism-a-runway-revolution-is-coming-to-the-bay-area-2#utm_source=rss&utm_medium=rss&utm_campaign=event-preview-fashion-v-fascism-a-runway-revolution-is-coming-to-the-bay-area-2)
- [Event Preview: ‘Fashion v. Fascism’ – A Runway Revolution Is Coming to the Bay Area](https://www.postnewsgroup.com/event-preview-fashion-v-fascism-a-runway-revolution-is-coming-to-the-bay-area#utm_source=rss&utm_medium=rss&utm_campaign=event-preview-fashion-v-fascism-a-runway-revolution-is-coming-to-the-bay-area)

### 214. 2 items; pair scores 98.9%–98.9% (mean 98.9%, 1 qualifying pairs)

Feed: <https://www.alexselimov.com/index.xml>

- [Blogs I like](https://www.alexselimov.com/blogs_i_like)
- [About](https://www.alexselimov.com/about)

### 215. 2 items; pair scores 98.9%–98.9% (mean 98.9%, 1 qualifying pairs)

Feed: <https://www.freaktakes.com/feed>

- [An Oral History Interview with ARIA CEO Ilan Gur](https://www.freaktakes.com/p/an-oral-history-interview-with-aria-3c4)
- [An Oral History Interview with ARIA CEO Ilan Gur \[Transcript\]](https://www.freaktakes.com/p/an-oral-history-interview-with-aria)

### 216. 2 items; pair scores 98.8%–98.8% (mean 98.8%, 1 qualifying pairs)

Feed: <https://books.uu.se/uup/gateway/plugin/WebFeedGatewayPlugin/rss2>

- [Euthymios the Iberian, Theodore of Edessa. Volume II](https://books.uu.se/uup/catalog/book/58)
- [Euthymios the Iberian, Theodore of Edessa. Volume I](https://books.uu.se/uup/catalog/book/57)

### 217. 2 items; pair scores 98.8%–98.8% (mean 98.8%, 1 qualifying pairs)

Feed: <https://joshblais.com/index.xml>

- [One Percent Per Day](https://joshblais.com/blog/1-percent-per-day)
- [One Percent Per Day](https://joshblais.com/blog/one-percent-per-day)

### 218. 2 items; pair scores 98.8%–98.8% (mean 98.8%, 1 qualifying pairs)

Feed: <https://posthog.com/rss.xml>

- [Winning from the back - late mover advantage](https://posthog.com/blog/ceo-diary-1)
- [Winning from the back - late mover advantage](https://posthog.com/founders/ceo-diary-1)

### 219. 2 items; pair scores 98.8%–98.8% (mean 98.8%, 1 qualifying pairs)

Feed: <https://www.sdr-radio.com/feed/rss2>

- [SDR Console, Beta Feb 10th 2026](https://www.sdr-radio.com/sdr-console-beta-feb-10th-2026)
- [SDR Console, Beta Nov 1st 2025](https://www.sdr-radio.com/sdr-console-beta-nov-1st-2025)

### 220. 2 items; pair scores 98.8%–98.8% (mean 98.8%, 1 qualifying pairs)

Feed: <https://feeds.npr.org/510313/podcast.xml>

- [Advice Line with Leah Solivan of Taskrabbit (September 2024)](https://rss.art19.com/episodes/638dce22-4382-41cd-8843-d53e701e7a51.mp3?rss_browser=bahjigtdahjvbwugogzfva%3d%3d--d05363d83ce333c74f32188013892b2863ad051c)
- [Advice Line with Leah Solivan of Taskrabbit](https://rss.art19.com/episodes/5750344b-e679-416d-84bc-0d0c76fbdabe.mp3?rss_browser=bahjigtdahjvbwugogzfva%3d%3d--d05363d83ce333c74f32188013892b2863ad051c)

### 221. 2 items; pair scores 98.7%–98.7% (mean 98.7%, 1 qualifying pairs)

Feed: <https://brainsteam.co.uk/index.xml>

- [Weeknote 13-19 August 2023](https://brainsteam.co.uk/2023/08/20/posts-2023-08-20-weeknote-13-19-august-20231692550760)
- [Weeknote 13-19 August 2023](https://brainsteam.co.uk/posts/2023/08/20/weeknote-13-19-august-20231692550760)

### 222. 2 items; pair scores 98.7%–98.7% (mean 98.7%, 1 qualifying pairs)

Feed: <https://ducttape.libsyn.com/rss>

- [AI, Content Strategy, and Building a Brand That Lasts](https://ducttape.libsyn.com/ai-content-strategy-and-building-a-brand-that-lasts)
- [How AI Is Redefining Content Strategy](https://ducttape.libsyn.com/how-ai-is-redefining-content-strategy)

### 223. 2 items; pair scores 98.6%–98.6% (mean 98.6%, 1 qualifying pairs)

Feed: <https://redocly.com/docs/changelog/feed.xml>

- [@redocly/theme 0.69.0](https://redocly.com/docs/realm/changelog#%40redocly%2ftheme%400.69.0)
- [@redocly/theme-experimental 0.20.0](https://redocly.com/docs/realm/changelog#%40redocly%2ftheme-experimental%400.20.0)

### 224. 2 items; pair scores 98.6%–98.6% (mean 98.6%, 1 qualifying pairs)

Feed: <https://starlabs.sg/index.xml>

- [(CVE-2023-3513) RazerCentralService unsafe deserialization Escalation of Privilege Vulnerability](https://starlabs.sg/advisories/23/23-3513)
- [(CVE-2023-3514) RazerCentralSerivce unsafe NamedPipe permission Escalation of Privilege Vulnerability](https://starlabs.sg/advisories/23/23-3514)

### 225. 2 items; pair scores 98.6%–98.6% (mean 98.6%, 1 qualifying pairs)

Feed: <https://www.bettedangerous.com/feed>

- [TODAY: Bette’s Happy Hour-Fundraiser for Lifeline Ukraine, Tuesday, September 15, 11 am Pacific](https://www.bettedangerous.com/p/today-bettes-happy-hour-fundraiser)
- [REGISTER: Bette’s Happy Hour-Fundraiser for Lifeline Ukraine, Tuesday, September 15, 11 am Pacific](https://www.bettedangerous.com/p/register-bettes-happy-hour-fundraiser)

### 226. 2 items; pair scores 98.6%–98.6% (mean 98.6%, 1 qualifying pairs)

Feed: <https://www.konichivalue.com/feed>

- [“I'd put all my money in South Korea, but I'd never touch the KOSPI” | 10 Fast with Michael Fritzell](https://www.konichivalue.com/p/id-put-all-my-money-in-south-korea)
- [“I'd put all my money in South Korea, but I'd never touch the KOSPI” | 10 Fast with Michael Fritzell](https://www.konichivalue.com/p/id-put-all-my-money-in-south-korea-a53)

### 227. 2 items; pair scores 98.5%–98.5% (mean 98.5%, 1 qualifying pairs)

Feed: <https://rss.beehiiv.com/feeds/nczRb4PQ6t.xml>

- [How LinkedIn Scaled Their System to 5 Million Queries Per Second](https://quastor.beehiiv.com/p/how-linkedin-scaled-their-system-to-5-million-queries-per-second-f8fe)
- [How LinkedIn Scaled Their System to 5 Million Queries Per Second](https://quastor.beehiiv.com/p/how-linkedin-scaled-their-system-to-5-million-queries-per-second)

### 228. 2 items; pair scores 98.5%–98.5% (mean 98.5%, 1 qualifying pairs)

Feed: <https://rss.art19.com/tim-ferriss-show>

- [Ep 38: Tony Robbins (Part 2) on Morning Routines, Peak Performance, and Mastering Money](https://rss.art19.com/episodes/f62665b8-d953-4f60-8d65-b398027143c3.mp3?rss_browser=bahjigtdahjvbwugogzfva%3d%3d--d05363d83ce333c74f32188013892b2863ad051c)
- [Ep 37: Tony Robbins on Morning Routines, Peak Performance, and Mastering Money](https://rss.art19.com/episodes/f5ac1312-ffc0-42e7-a693-0d8a32e5dc58.mp3?rss_browser=bahjigtdahjvbwugogzfva%3d%3d--d05363d83ce333c74f32188013892b2863ad051c)

### 229. 2 items; pair scores 98.5%–98.5% (mean 98.5%, 1 qualifying pairs)

Feed: <https://www.vibewire.com.au/?feed=rss2>

- [Mysterious object appears during Starship flight](https://www.vibewire.com.au/?p=560340&utm_source=rss&utm_medium=rss&utm_campaign=mysterious-object-appears-during-starship-flight-2)
- [Mysterious object appears during Starship flight](https://www.vibewire.com.au/?p=560339&utm_source=rss&utm_medium=rss&utm_campaign=mysterious-object-appears-during-starship-flight)

### 230. 2 items; pair scores 98.3%–98.3% (mean 98.3%, 1 qualifying pairs)

Feed: <https://altairmedia.eu/feed/>

- [Europe Can Make Photonic Chips. Who Will Buy Them?](https://altairmedia.eu/europe-can-make-photonic-chips-who-will-buy-them-2)
- [Europe Can Make Photonic Chips. Who Will Buy Them?](https://altairmedia.eu/europe-can-make-photonic-chips-who-will-buy-them)

### 231. 2 items; pair scores 98.3%–98.3% (mean 98.3%, 1 qualifying pairs)

Feed: <https://rss.art19.com/tim-ferriss-show>

- [Ep 34: Ramit Sethi (Part 2) on Persuasion, Negotiation, and Turning a Blog Into a Multi-Million-Dollar Business](https://rss.art19.com/episodes/389a49cc-4fe4-4dd7-9f2d-17e2f9062fef.mp3?rss_browser=bahjigtdahjvbwugogzfva%3d%3d--d05363d83ce333c74f32188013892b2863ad051c)
- [Ep 33: Ramit Sethi on Persuasion, Negotiation, and Turning a Blog Into a Multi-Million-Dollar Business](https://rss.art19.com/episodes/7c653f0f-800c-48b8-bfbb-d36906a1e0b2.mp3?rss_browser=bahjigtdahjvbwugogzfva%3d%3d--d05363d83ce333c74f32188013892b2863ad051c)

### 232. 2 items; pair scores 98.2%–98.2% (mean 98.2%, 1 qualifying pairs)

Feed: <https://www.bettedangerous.com/feed>

- [TODAY: Bette’s Tuesday Happy Hour with Joni Askola and Friends](https://www.bettedangerous.com/p/today-bettes-tuesday-happy-hour-with-1ce)
- [REGISTRATION: Bette’s Tuesday Happy Hour with Joni Askola and Friends](https://www.bettedangerous.com/p/registration-bettes-tuesday-happy-dae)

### 233. 2 items; pair scores 98.2%–98.2% (mean 98.2%, 1 qualifying pairs)

Feed: <https://www.chalkbeat.org/arc/outboundfeeds/rss/>

- [Could Newark’s universal enrollment system teach Philadelphia some lessons?](https://www.chalkbeat.org/newark/2026/09/23/could-universal-enrollment-for-schools-fix-selective-admissions-process)
- [Philadelphia’s school enrollment system is infamously complicated. Could a universal application help?](https://www.chalkbeat.org/philadelphia/2026/09/23/could-universal-enrollment-for-schools-fix-selective-admissions-process)

### 234. 2 items; pair scores 98.2%–98.2% (mean 98.2%, 1 qualifying pairs)

Feed: <https://simianwords.bearblog.dev/feed/?type=rss>

- [I tried all AI voice assistants and Grok won](https://simianwords.bearblog.dev/i-tried-all-ai-voice-assistants-and-grok-won)
- [The state of AI voice assistants is bad but there's a clear winner](https://simianwords.bearblog.dev/the-state-of-ai-voice-assistants-is-bad-but-theres-a-clear-winner)

### 235. 2 items; pair scores 98.2%–98.2% (mean 98.2%, 1 qualifying pairs)

Feed: <https://rss.beehiiv.com/feeds/nczRb4PQ6t.xml>

- [The Architecture of Canva's Data Platform](https://quastor.beehiiv.com/p/the-architecture-of-canva-s-data-platform-b20a)
- [The Architecture of Canva's Data Platform](https://quastor.beehiiv.com/p/the-architecture-of-canva-s-data-platform)

### 236. 2 items; pair scores 98.0%–98.0% (mean 98.0%, 1 qualifying pairs)

Feed: <https://nintendowire.com/feed/>

- [Pokémon GO Showcase Tuesday for Tuesday, September 29th, 2026](https://nintendowire.com/guides/pokemon-go/showcase-tuesday-for-september-29th-2026)
- [Pokémon GO Showcase Tuesday for Tuesday, September 22nd, 2026](https://nintendowire.com/guides/pokemon-go/showcase-tuesday-for-september-22nd-2026)

### 237. 2 items; pair scores 98.0%–98.0% (mean 98.0%, 1 qualifying pairs)

Feed: <https://rss.simplecast.com/podcasts/7838/rss>

- [Tristen Chernove, the rider with 13 rainbow jerseys and three Paralympic medals, on the Games \[rebroadcast\]](https://cyclingmagazine.ca/)
- [Tristen Chernove, the rider with 13 rainbow jerseys and three Paralympic medals, looks ahead to the Games](https://cyclingmagazine.ca/)

### 238. 2 items; pair scores 98.0%–98.0% (mean 98.0%, 1 qualifying pairs)

Feed: <https://ducttape.libsyn.com/rss>

- [Marketing Chaos Ends With a Real System](https://ducttape.libsyn.com/marketing-chaos-ends-with-a-real-system-1)
- [Marketing Chaos Ends With a Real System](https://ducttape.libsyn.com/marketing-chaos-ends-with-a-real-system)

### 239. 2 items; pair scores 98.0%–98.0% (mean 98.0%, 1 qualifying pairs)

Feed: <https://budgetmodel.wharton.upenn.edu/rss.xml>

- [President Trump-Signed Reconciliation Bill (OBBBA): Budget, Economic, and Distributional Effects](https://budgetmodel.wharton.upenn.edu/p/2025-07-08-president-trump-signed-reconciliation-bill)
- [Senate-Passed Reconciliation Bill (OBBBA) Budget, Economic, and Distributional Effects](https://budgetmodel.wharton.upenn.edu/p/2025-07-01-senate-reconciliation-bill-budget-economic-and-distributional-effects-june-29-2025)

### 240. 2 items; pair scores 97.9%–97.9% (mean 97.9%, 1 qualifying pairs)

Feed: <https://feeds.npr.org/510313/podcast.xml>

- [Advice Line with Tim Ferriss (August 2025)](https://rss.art19.com/episodes/84f10963-70ff-444d-87df-cfedab442943.mp3?rss_browser=bahjigtdahjvbwugogzfva%3d%3d--d05363d83ce333c74f32188013892b2863ad051c)
- [Advice Line with Tim Ferriss](https://rss.art19.com/episodes/444c8167-0686-4ae3-ad0d-217dd492726b.mp3?rss_browser=bahjigtdahjvbwugogzfva%3d%3d--d05363d83ce333c74f32188013892b2863ad051c)

### 241. 2 items; pair scores 97.8%–97.8% (mean 97.8%, 1 qualifying pairs)

Feed: <https://dylanbeattie.net/rss.xml>

- [Spotlight, Dynamics CRM, and the age-old question of “build vs buy”](https://dylanbeattie.net/2015/04/24/spotlight-dynamics-crm-and-age-old_24.html)
- [Spotlight, Dynamics CRM, and the age-old question of “build vs buy”](https://dylanbeattie.net/2015/04/24/spotlight-dynamics-crm-and-age-old.html)

### 242. 2 items; pair scores 97.8%–97.8% (mean 97.8%, 1 qualifying pairs)

Feed: <https://brainsteam.co.uk/index.xml>

- [Weeknote Week 39 2023](https://brainsteam.co.uk/2023/10/01/2023-10-1-weeknote-39)
- [Weeknote Week 39 2023](https://brainsteam.co.uk/2023/10/1/weeknote-39)

### 243. 2 items; pair scores 97.7%–97.7% (mean 97.7%, 1 qualifying pairs)

Feed: <https://multitudes.weisser.io/feed>

- [Solving Chronic Pain and Long COVID: The Mind-Body Connection with Dr. Michael Donnino](https://multitudes.weisser.io/p/solving-chronic-pain-and-long-covid-cd9)
- [Solving Chronic Pain and Long COVID: The Mind-Body Connection with Dr. Michael Donnino](https://multitudes.weisser.io/p/solving-chronic-pain-and-long-covid)

### 244. 2 items; pair scores 97.6%–97.6% (mean 97.6%, 1 qualifying pairs)

Feed: <https://feeds.simplecast.com/dLRotFGk>

- [Welcome Back Interview with Ernie Miller, Head of Engineering at Monograph, Part Two](http://www.developertea.com)
- [Welcome Back Interview with Ernie Miller, Head of Engineering at Monograph, Part One](http://www.developertea.com)

### 245. 2 items; pair scores 97.6%–97.6% (mean 97.6%, 1 qualifying pairs)

Feed: <https://nintendowire.com/feed/>

- [Nintendo Switch 2 Pre-Order Deals Guide: Even More Games Added! (Ocarina of Time, Metroid)](https://nintendowire.com/features/nintendo-switch-2-pre-order-deals-guide-ocarina-of-time-metroid-more-updated)
- [Nintendo Switch 2 Pre-Order Deals Guide: Ocarina of Time, Metroid & More](https://nintendowire.com/features/nintendo-switch-2-pre-order-deals-guide-ocarina-of-time-metroid-more)

### 246. 2 items; pair scores 97.6%–97.6% (mean 97.6%, 1 qualifying pairs)

Feed: <https://rss.art19.com/tim-ferriss-show>

- [Ep 42: Rolf Potts (Part 2) on Travel Tactics, Creating Time Wealth, and Lateral Thinking](https://rss.art19.com/episodes/1953395c-a0cf-4abb-83f6-bb4475ae7988.mp3?rss_browser=bahjigtdahjvbwugogzfva%3d%3d--d05363d83ce333c74f32188013892b2863ad051c)
- [Ep 41: Rolf Potts on Travel Tactics, Creating Time Wealth, and Lateral Thinking](https://rss.art19.com/episodes/5393f92d-9f35-41d9-82a4-f1668150d8f1.mp3?rss_browser=bahjigtdahjvbwugogzfva%3d%3d--d05363d83ce333c74f32188013892b2863ad051c)

### 247. 2 items; pair scores 97.5%–97.5% (mean 97.5%, 1 qualifying pairs)

Feed: <https://www.loodfun.com/feed/>

- [L’IA en entreprise : elle ne remplace personne, elle fait gagner un temps fou.](https://www.loodfun.com/lia-en-entreprise-elle-ne-remplace-personne-elle-fait-gagner-un-temps-fou)
- [Pourquoi certaines invitations donnent immédiatement envie de répondre ?](https://www.loodfun.com/pourquoi-certaines-invitations-donnent-immediatement-envie-de-repondre)

### 248. 2 items; pair scores 97.4%–97.4% (mean 97.4%, 1 qualifying pairs)

Feed: <https://brainsteam.co.uk/index.xml>

- [Weeknote CW 41 2023](https://brainsteam.co.uk/2023/10/15/posts-2023-10-15-weeknote-cw-41-20231697388830)
- [Weeknote CW 41 2023](https://brainsteam.co.uk/posts/2023/10/15/weeknote-cw-41-20231697388830)

### 249. 2 items; pair scores 97.4%–97.4% (mean 97.4%, 1 qualifying pairs)

Feed: <https://themedium.ca/feed/>

- [More than a ball game How a $15 Bus Trip Brought UTM to Rogers Centre](https://themedium.ca/more-than-a-ball-game)
- [Why We Need a Little More Dolly  A look at the legacy Dolly Parton has left behind, and what we can learn from it.](https://themedium.ca/why-we-need-a-little-more-dolly)

### 250. 2 items; pair scores 97.4%–97.4% (mean 97.4%, 1 qualifying pairs)

Feed: <https://grumpygamer.com/index.xml>

- [Lock-down in Seattle](https://grumpygamer.com/lock_down)
- [The CORVID-19 edition](https://grumpygamer.com/confinded_to_home)

### 251. 2 items; pair scores 97.3%–97.3% (mean 97.3%, 1 qualifying pairs)

Feed: <https://vinitkumar.me/rss.xml>

- [How To Convert LaTex to PDF on macOS](https://vinitkumar.me/2019-01-16-converting-latex-to-pdf-on-macos)
- [Lightweight LaTeX to PDF Conversion on macOS: A Minimal Setup Guide](https://vinitkumar.me/converting-latex-to-pdf-on-macos)

### 252. 2 items; pair scores 97.2%–97.2% (mean 97.2%, 1 qualifying pairs)

Feed: <https://techaccelerationandresilience.com/blog-posts?format=rss>

- [We don’t have to find Permit A38: better faster Incident Management](https://techaccelerationandresilience.com/blog-posts/we-dont-have-to-find-permit-a38-better-faster-incident-management)
- [We don’t have to find Permit A38: better faster Incident Management](https://techaccelerationandresilience.com/blog-posts/fukcecslwt44o6dzux1scvfw6advu1)

### 253. 2 items; pair scores 97.1%–97.1% (mean 97.1%, 1 qualifying pairs)

Feed: <https://starlabs.sg/index.xml>

- [(CVE-2021-4206) QEMU QXL Integer overflow leads to Heap Overflow](https://starlabs.sg/advisories/21/21-4206)
- [(CVE-2021-4207) QEMU QXL Integer overflow leads to Heap Overflow](https://starlabs.sg/advisories/21/21-4207)

### 254. 2 items; pair scores 97.1%–97.1% (mean 97.1%, 1 qualifying pairs)

Feed: <https://quarkus.io/feed.xml>

- [Quarkus 3.27.5.3 released - LTS emergency release](https://quarkus.io/blog/quarkus-3-27-5-3-released)
- [Quarkus 3.33.3.3 released - LTS emergency release](https://quarkus.io/blog/quarkus-3-33-3-3-released)

### 255. 2 items; pair scores 97.1%–97.1% (mean 97.1%, 1 qualifying pairs)

Feed: <https://metr.org/feed.xml>

- [前沿 AI 风险报告（2026 年 2–3 月）](https://metr.org/zh-hans/blog/2026-05-19-frontier-risk-report)
- [Frontier Risk Report (February to March 2026)](https://metr.org/blog/2026-05-19-frontier-risk-report)

### 256. 2 items; pair scores 97.0%–97.0% (mean 97.0%, 1 qualifying pairs)

Feed: <https://brainsteam.co.uk/index.xml>

- [Prod-Ready Airbyte Sync](https://brainsteam.co.uk/2023/8/14/stable-airbyte-sync)
- [Prod-Ready Airbyte Sync](https://brainsteam.co.uk/2023/08/14/stable-airbyte-sync)

### 257. 2 items; pair scores 97.0%–97.0% (mean 97.0%, 1 qualifying pairs)

Feed: <https://quarkus.io/feed.xml>

- [Quarkus 3.27.5 released - LTS maintenance release](https://quarkus.io/blog/quarkus-3-27-5-released)
- [Quarkus 3.33.3 released - LTS maintenance release](https://quarkus.io/blog/quarkus-3-33-3-released)

### 258. 2 items; pair scores 96.8%–96.8% (mean 96.8%, 1 qualifying pairs)

Feed: <https://www.nj.com/arc/outboundfeeds/rss/?outputType=xml>

- [Daily field hockey stat leaders for Wednesday, Sept. 30](https://www.nj.com/highschoolsports/2026/10/daily-field-hockey-stat-leaders-for-wednesday-sept-30.html)
- [Essex-Union Conference field hockey season stats leaders for Oct. 1](https://www.nj.com/highschoolsports/2026/10/essex-union-conference-field-hockey-season-stats-leaders-for-oct-1.html)

### 259. 2 items; pair scores 96.8%–96.8% (mean 96.8%, 1 qualifying pairs)

Feed: <https://www.senki.org/feed/>

- [FAQ – Which Shadowserver Reports list CVEs](https://www.senki.org/faq-which-shadowserver-reports-list-cves-2)
- [FAQ – Which Shadowserver Reports list CVEs](https://www.senki.org/faq-which-shadowserver-reports-list-cves)

### 260. 2 items; pair scores 96.8%–96.8% (mean 96.8%, 1 qualifying pairs)

Feed: <https://cassidoo.co/rss.xml>

- [Things you should have on your LinkedIn profile](https://cassidoo.co/post/linkedin-profile)
- [Things you should have on your LinkedIn profile](https://cassidoo.co/post/linkedin-profile-things)

### 261. 2 items; pair scores 96.6%–96.6% (mean 96.6%, 1 qualifying pairs)

Feed: <https://etcd.io/index.xml>

- [Libraries and tools](https://etcd.io/docs/v3.2/integrations)
- [Libraries and tools](https://etcd.io/docs/v3.3/integrations)

### 262. 2 items; pair scores 96.6%–96.6% (mean 96.6%, 1 qualifying pairs)

Feed: <https://liu.se/rss/liu-jobs-sv.rss>

- [Serviceinriktad IT-tekniker med AI-kompetens](https://web103.reachmee.com/ext/i011/853/main?site=6&validator=c5f766a55eafbb016232008485a24b49&lang=se&rmpage=job&rmjob=29862)
- [Serviceinriktad IT-tekniker med AI-kompetens, vikariat](https://web103.reachmee.com/ext/i011/853/main?site=6&validator=c5f766a55eafbb016232008485a24b49&lang=se&rmpage=job&rmjob=29863)

### 263. 2 items; pair scores 96.6%–96.6% (mean 96.6%, 1 qualifying pairs)

Feed: <https://feeds.npr.org/510313/podcast.xml>

- [Advice Line with Marcia Kilgore of Beauty Pie (June 2025)](https://rss.art19.com/episodes/fc2fd26c-9fb7-49f2-b64b-65853699e45a.mp3?rss_browser=bahjigtdahjvbwugogzfva%3d%3d--d05363d83ce333c74f32188013892b2863ad051c)
- [Advice Line with Marcia Kilgore of Beauty Pie](https://rss.art19.com/episodes/59e88860-94f4-4e3c-8fe6-0e6acd65cfb8.mp3?rss_browser=bahjigtdahjvbwugogzfva%3d%3d--d05363d83ce333c74f32188013892b2863ad051c)

### 264. 2 items; pair scores 96.5%–96.5% (mean 96.5%, 1 qualifying pairs)

Feed: <http://feeds.feedburner.com/psblog>

- [(For SEA) God of War Laufey pre-orders available starting September 29, editions detailed](https://blog.playstation.com/2026/09/28/20260929-gow)
- [God of War Laufey pre-orders available starting September 29, editions detailed](https://blog.playstation.com/2026/09/28/god-of-war-laufey-pre-orders-available-starting-september-29-editions-detailed)

### 265. 2 items; pair scores 96.4%–96.4% (mean 96.4%, 1 qualifying pairs)

Feed: <https://nintendowire.com/feed/>

- [Pokémon GO Raid Hour for Wednesday, September 30th, 2026](https://nintendowire.com/guides/pokemon-go/raid-hour-for-september-30th-2026)
- [Pokémon GO Raid Hour for Wednesday, September 23rd, 2026](https://nintendowire.com/guides/pokemon-go/raid-hour-for-september-23rd-2026)

### 266. 2 items; pair scores 96.2%–96.2% (mean 96.2%, 1 qualifying pairs)

Feed: <https://danariely.com/feed/>

- [Which Country Would You Choose, If You Didn’t Know Who You’d Be Born As?](https://danariely.com/which-country-would-you-choose-if-you-didnt-know-who-youd-be-born-as-2)
- [Which Country Would You Choose, If You Didn't Know Who You'd Be Born As?](https://danariely.com/which-country-would-you-choose-if-you-didnt-know-who-youd-be-born-as)

### 267. 2 items; pair scores 96.0%–96.0% (mean 96.0%, 1 qualifying pairs)

Feed: <https://www.aloneguid.uk/index.xml>

- [Ultimate Dev Tools List for 2025](https://www.aloneguid.uk/posts/2025/01/ultimate-dev-tools-list)
- [Ultimate Dev Tools List for 2024](https://www.aloneguid.uk/posts/2024/01/ultimate-dev-tools-list)

### 268. 2 items; pair scores 96.0%–96.0% (mean 96.0%, 1 qualifying pairs)

Feed: <https://www.whitehouse.gov/presidential-actions/feed/>

- [Modifying the Scope of Products of Canada Subject to the Additional Duties Imposed to Offset Canadian Discrimination Against the United States with Respect to Motor Vehicles](https://www.whitehouse.gov/presidential-actions/2026/09/modifying-the-scope-of-products-of-canada-subject-to-the-additional-duties-imposed-to-offset-canadian-discrimination-against-the-united-states-with-respect-to-motor-vehicles)
- [Modifying the Scope of Products of Canada Subject to the Additional Duties Imposed to Offset Canadian Discrimination Against the Commerce of the United States with Respect to Alcoholic Beverages](https://www.whitehouse.gov/presidential-actions/2026/09/modifying-the-scope-of-products-of-canada-subject-to-the-additional-duties-imposed-to-offset-canadian-discrimination-against-the-commerce-of-the-united-states-with-respect-to-alcoholic-beverages)

### 269. 2 items; pair scores 95.8%–95.8% (mean 95.8%, 1 qualifying pairs)

Feed: <https://www.euzoia.org/feed>

- [Work | How to build a strong network (with Mark Moore)](https://www.euzoia.org/p/work-how-to-build-a-strong-network-55f)
- [Work | How to Navigate your Career (With Sofia Balderson)](https://www.euzoia.org/p/work-how-to-navigate-your-career-c71)

### 270. 2 items; pair scores 95.8%–95.8% (mean 95.8%, 1 qualifying pairs)

Feed: <https://brainsteam.co.uk/index.xml>

- [We moved offices!](https://brainsteam.co.uk/posts/2023/08/28/we-moved-offices1693231919)
- [We moved offices!](https://brainsteam.co.uk/2023/08/08/we-moved-offices)

### 271. 2 items; pair scores 95.8%–95.8% (mean 95.8%, 1 qualifying pairs)

Feed: <https://ijr.com/feed.xml>

- [Flydubai flight to Tel Aviv diverts to Saudi Arabia after pilot altercation](https://ijr.com/journals/faithtap/flydubai-flight-to-tel-aviv-diverts-to-saudi-arabia-after-pilot-altercation-d9c6beef)
- [Flydubai flight to Tel Aviv diverts to Saudi Arabia after pilot altercation](https://ijr.com/discover/world-2026-09-30-flydubai-flight-diverts-saudi-arabia-1-04bb10b3)

### 272. 2 items; pair scores 95.6%–95.6% (mean 95.6%, 1 qualifying pairs)

Feed: <https://brainsteam.co.uk/index.xml>

- [Dealing with death-by-a-thousand questions at work](https://brainsteam.co.uk/posts/2023/10/18/dealing-with-death-by-a-thousand-questions-at-work1697619891)
- [Dealing with death-by-a-thousand questions in the workplace](https://brainsteam.co.uk/2023/10/18/dealing-with-death-by-a-thousand-questions-in-the-workplace)

### 273. 2 items; pair scores 95.3%–95.3% (mean 95.3%, 1 qualifying pairs)

Feed: <https://blog.railway.com/rss.xml>

- [Incident Report: March 30th, 2026 — Authenticated user data cached](https://blog.railway.com/p/incident-report-march-30-2026-authenticated-user-data-cached)
- [Incident Report: March 30th, 2026 — Authenticated user data cached](https://blog.railway.com/p/incident-report-march-30-2026-accidental-cdn-caching)

### 274. 2 items; pair scores 95.3%–95.3% (mean 95.3%, 1 qualifying pairs)

Feed: <https://mp1st.com/feed/>

- [Diablo IV Version 3.2.2 Now Out for Update 1.119](https://mp1st.com/title-updates-and-patches/diablo-iv-version-3-2-2-now-out-for-update-1-119)
- [Diablo 4 Update 2.19 Brings Season of Hell’s Legacy Fixes on September 30](https://mp1st.com/title-updates-and-patches/diablo-4-version-3-2-2-brings-season-of-hells-legacy-fixesseptember-30-update-2-19-1-119)

### 275. 2 items; pair scores 95.2%–95.2% (mean 95.2%, 1 qualifying pairs)

Feed: <https://feeds.npr.org/510313/podcast.xml>

- [Advice Line with Jack Conte of Patreon (December 2024)](https://rss.art19.com/episodes/d3b52293-6827-403d-a4a4-6c6ec5710ef0.mp3?rss_browser=bahjigtdahjvbwugogzfva%3d%3d--d05363d83ce333c74f32188013892b2863ad051c)
- [Advice Line with Jack Conte of Patreon](https://rss.art19.com/episodes/5b284321-e27b-4731-9bf2-c05f293d4315.mp3?rss_browser=bahjigtdahjvbwugogzfva%3d%3d--d05363d83ce333c74f32188013892b2863ad051c)

### 276. 2 items; pair scores 95.1%–95.1% (mean 95.1%, 1 qualifying pairs)

Feed: <https://ijr.com/feed.xml>

- [Vithya Ramraj surges past China on the anchor leg as India takes Asian Games relay gold](https://ijr.com/journals/faithtap/vithya-ramraj-surges-past-china-on-the-anchor-leg-as-india-takes-asian-games-rel-a7bfc975)
- [Vithya Ramraj surges past China on the anchor leg as India takes Asian Games relay gold](https://ijr.com/discover/vithya-ramraj-surges-past-china-on-the-anchor-leg-as-india-t-3e7695d0)

### 277. 2 items; pair scores 94.9%–94.9% (mean 94.9%, 1 qualifying pairs)

Feed: <https://brainsteam.co.uk/index.xml>

- [Turbopilot - a Retrospective](https://brainsteam.co.uk/2023/09/30/turbopilot-obit)
- [Turbopilot - a Retrospective](https://brainsteam.co.uk/posts/2023/09/30/turbopilot-obit)

### 278. 2 items; pair scores 94.8%–94.8% (mean 94.8%, 1 qualifying pairs)

Feed: <http://blog.netbsd.org/tnf/feed/entries/rss>

- [NetBSD 11.0 RC3 available!](http://blog.netbsd.org/tnf/entry/netbsd_11_0_rc3_available)
- [NetBSD 11.0 RC2 available!](http://blog.netbsd.org/tnf/entry/netbsd_11_0_rc2_available)

### 279. 2 items; pair scores 94.6%–94.6% (mean 94.6%, 1 qualifying pairs)

Feed: <https://electronictradinghub.com/feed/>

- [How to design high-frequency trading systems and its architecture. Part I](https://electronictradinghub.com/how-to-design-high-frequency-trading-systems-and-their-architecture-part-i)
- [How do I design high-frequency trading systems and its architecture. Part I](https://electronictradinghub.com/how-do-i-design-high-frequency-trading-systems-and-its-architecture-part-i)

### 280. 2 items; pair scores 94.6%–94.6% (mean 94.6%, 1 qualifying pairs)

Feed: <https://mp1st.com/feed/>

- [Fortnite Update 42.30 for Fortnitemares 2026 Now Live via Patch 1.000.233](https://mp1st.com/title-updates-and-patches/fortnite-update-42-30-fortnitemares-2026-now-live-patch-1-000-233)
- [Fortnite Update 5.22 Drags Fortnitemares 2026 on October 1](https://mp1st.com/title-updates-and-patches/fortnite-update-5-22-drags-fortnitemares-2026-on-october-1)

### 281. 2 items; pair scores 94.5%–94.5% (mean 94.5%, 1 qualifying pairs)

Feed: <https://brainsteam.co.uk/index.xml>

- [Weeknote CW43 2023](https://brainsteam.co.uk/2023/10/29/weeknote-cw43-2023)
- [Weeknote CW43 2023](https://brainsteam.co.uk/posts/2023/10/29/weeknote-cw43-20231698573691)

### 282. 2 items; pair scores 94.3%–94.3% (mean 94.3%, 1 qualifying pairs)

Feed: <https://iridia.cat/en/feed/>

- [European Court of Human Rights agrees to examine application concerning ill-treatment at Barcelona’s Immigration Detention Centre](https://iridia.cat/en/el-tribunal-europeu-de-drets-humans-admet-a-tramit-una-demanda-per-maltractaments-al-cie-de-barcelona)
- [European Court of Human Rights agrees to examine application concerning ill-treatment at Barcelona’s Immigration Detention Centre](https://iridia.cat/en/european-court-of-human-rights-agrees-to-examine-application-concerning-ill-treatment-at-barcelonas-immigration-detention-centre)

### 283. 2 items; pair scores 94.2%–94.2% (mean 94.2%, 1 qualifying pairs)

Feed: <https://feeds.simplecast.com/dLRotFGk>

- [Katy Milkman, Author of How to Change and Host of Choiceology, Part Two](http://www.developertea.com)
- [(Fixed Audio) Katy Milkman, Author of How to Change and Host of Choiceology, Part One](http://www.developertea.com)

### 284. 2 items; pair scores 94.0%–94.0% (mean 94.0%, 1 qualifying pairs)

Feed: <http://feeds.wnyc.org/radiolab>

- [Oliver Sipple](https://www.radiolab.org)
- [Oliver Sipple](https://www.radiolab.org)

### 285. 2 items; pair scores 93.8%–93.8% (mean 93.8%, 1 qualifying pairs)

Feed: <https://farshid.co.uk/feed.xml>

- [Durable Workflow Engines - Ridiculously Jumper!](https://farshid.co.uk/entry/durable_workflow_engines_ridiculously_absurd)
- [Integrating `highway_dsl` and `Jumper` for a Durable Workflow Engine](https://farshid.co.uk/entry/integrating_highway_dsl_and_absurd_for_a_durable_workflow_engine)

### 286. 2 items; pair scores 93.6%–93.6% (mean 93.6%, 1 qualifying pairs)

Feed: <https://adventuresindevops.com/episodes/rss.xml>

- [Simplifying DevOps  - DevOps 229](https://adventuresindevops.com/episodes)
- [Simplifying DevOps - DevOps 099](https://adventuresindevops.com/episodes)

### 287. 2 items; pair scores 93.6%–93.6% (mean 93.6%, 1 qualifying pairs)

Feed: <https://feeds.simplecast.com/dLRotFGk>

- [Interview w/ Trevor Hinesley (Part 2)](http://www.developertea.com)
- [Interview w/ Trevor Hinesley (Part 1)](http://www.developertea.com)

### 288. 2 items; pair scores 93.5%–93.5% (mean 93.5%, 1 qualifying pairs)

Feed: <https://www.sdr-radio.com/feed/rss2>

- [SDR Television v1.0.1 (Bugfix)](https://www.sdr-radio.com/sdr-television-v1-0-1-bugfix)
- [SDR Television v1 (Release)](https://www.sdr-radio.com/sdr-television-v1-release)

### 289. 2 items; pair scores 93.5%–93.5% (mean 93.5%, 1 qualifying pairs)

Feed: <https://troz.net/index.xml>

- [macOS Apprentice: 3rd Edition](https://troz.net/post/2026/macos-apprentice-update-3)
- [macOS Apprentice Update](https://troz.net/post/2025/macos-apprentice-update)

### 290. 2 items; pair scores 93.1%–93.1% (mean 93.1%, 1 qualifying pairs)

Feed: <https://rss.art19.com/tim-ferriss-show>

- [#171: The Random Show - New Favorite Books, Memory Training, and Bets On VR](https://rss.art19.com/episodes/0507dfc4-474d-4016-9ffd-41d4490e8ad3.mp3?rss_browser=bahjigtdahjvbwugogzfva%3d%3d--d05363d83ce333c74f32188013892b2863ad051c)
- [#146: The Random Show, Ice Cold Edition](https://rss.art19.com/episodes/5b16c93f-7ce3-4b04-a24f-8d57e08e306d.mp3?rss_browser=bahjigtdahjvbwugogzfva%3d%3d--d05363d83ce333c74f32188013892b2863ad051c)

### 291. 2 items; pair scores 92.9%–92.9% (mean 92.9%, 1 qualifying pairs)

Feed: <https://www.radiotimes.com/feed>

- [Esther Rantzen: 'How would I like to be remembered? With love and laughter'](https://www.radiotimes.com/tv/entertainment/esther-rantzen-radio-times-magazine)
- [Dame Esther Rantzen: 'How I would like to be remembered? With love and laughter'](https://www.radiotimes.com/app/dame-esther-rantzen-how-i-would-like-to-be-remembered-with-love-and-laughter)

### 292. 2 items; pair scores 92.9%–92.9% (mean 92.9%, 1 qualifying pairs)

Feed: <https://medium.com/feed/@techandsundry>

- [The Illusion of Convenience](https://medium.com/intersectionality/the-allusion-of-convenience-138004cc2b9d?source=rss-4a3dbecd47d6------2)
- [The Allusion of Convenience Offered by Our Tech Oligarchs](https://techandsundry.medium.com/the-allusion-of-convenience-offered-by-our-tech-oligarchs-94a93dd39ae6?source=rss-4a3dbecd47d6------2)

### 293. 2 items; pair scores 92.6%–92.6% (mean 92.6%, 1 qualifying pairs)

Feed: <https://blog.ifcomp.org/rss>

- [IFComp Seeking an Artist for the 2025 Logo](https://blog.ifcomp.org/post/785511692459294720)
- [IFComp Seeking an Artist for the 2024 Logo](https://blog.ifcomp.org/post/755150987967266816)

### 294. 2 items; pair scores 92.5%–92.5% (mean 92.5%, 1 qualifying pairs)

Feed: <https://press-start.com.au/feed/>

- [All The Aussie Times For Tonight’s iPhone 18 Event And Where To Watch](https://press-start.com.au/news/tech-news/2026/09/09/all-the-aussie-times-for-tonights-iphone-18-event-and-where-to-watch)
- [The iPhone 18 Event Is Happening This Week And Here’s All The Aussie Times](https://press-start.com.au/news/tech-news/2026/09/07/the-iphone-18-event-is-happening-this-week-and-heres-all-the-aussie-times)

### 295. 2 items; pair scores 92.4%–92.4% (mean 92.4%, 1 qualifying pairs)

Feed: <https://www.revenuemodel.ai/rss/>

- [Tokens don’t measure what matters, so change the measurement](https://www.revenuemodel.ai/dont-change-the-model-change-the-unit-of-measure)
- [Tokens can’t track AI cost at scale. FBM can — without changing the model](https://www.revenuemodel.ai/tokens-cant-track-ai-cost-at-scale-fbm-can-without-changing-the-model)

### 296. 2 items; pair scores 92.4%–92.4% (mean 92.4%, 1 qualifying pairs)

Feed: <https://totaltogether.com/feed/>

- [Collaborate with Us](https://totaltogether.com/collaborate-with-us-2028)
- [Partner with Us](https://totaltogether.com/partner-with-us-2027)

### 297. 2 items; pair scores 92.4%–92.4% (mean 92.4%, 1 qualifying pairs)

Feed: <https://pypy.org/rss.xml>

- [PyPy v7.3.21 release](https://www.pypy.org/posts/2026/03/pypy-v7321-release.html)
- [PyPy v7.3.20 release](https://www.pypy.org/posts/2025/07/pypy-v7320-release.html)

### 298. 2 items; pair scores 92.4%–92.4% (mean 92.4%, 1 qualifying pairs)

Feed: <https://www.pypy.org/rss.xml>

- [PyPy v7.3.21 release](https://www.pypy.org/posts/2026/03/pypy-v7321-release.html)
- [PyPy v7.3.20 release](https://www.pypy.org/posts/2025/07/pypy-v7320-release.html)

### 299. 2 items; pair scores 92.2%–92.2% (mean 92.2%, 1 qualifying pairs)

Feed: <http://feeds.thememorypalace.us/thememorypalace>

- [A White Horse](https://play.prx.org/listen?ge=prx_3_96ab98bb-9ffd-463b-bde2-89f3bb3ed5f1&uf=http%3a%2f%2ffeeds.thememorypalace.us%2fthememorypalace)
- [Episode 90 (A White Horse)](https://play.prx.org/listen?ge=862aa2568afd632eef63756e659b25d3&uf=http%3a%2f%2ffeeds.thememorypalace.us%2fthememorypalace)

### 300. 2 items; pair scores 91.7%–91.7% (mean 91.7%, 1 qualifying pairs)

Feed: <https://www.sdr-radio.com/feed/rss2>

- [SDR Console, Beta August 27th 2026](https://www.sdr-radio.com/sdr-console-beta-august-27th-2026)
- [SDR Console, Beta July 26th 2026](https://www.sdr-radio.com/sdr-console-beta-july-26th-2026)

### 301. 2 items; pair scores 91.6%–91.6% (mean 91.6%, 1 qualifying pairs)

Feed: <https://investorplace.com/content-feed/>

- [Wall Street’s Tom Brady Is Still Flying Under the Radar](https://investorplace.com/smartmoney/2026/09/wall-streets-tom-brady-is-still-flying-under-the-radar)
- [The Small-Cap Comeback May Be Earlier Than It Looks](https://investorplace.com/hypergrowthinvesting/2026/09/the-small-cap-comeback-may-be-earlier-than-it-looks)

### 302. 2 items; pair scores 91.5%–91.5% (mean 91.5%, 1 qualifying pairs)

Feed: <https://www.sdr-radio.com/feed/rss2>

- [Simon's World Map 1.5.4](https://www.sdr-radio.com/simon-s-world-map-1-5-4)
- [Simon's World Map 1.5.2](https://www.sdr-radio.com/simon-s-world-map-1-5-2)

### 303. 2 items; pair scores 91.4%–91.4% (mean 91.4%, 1 qualifying pairs)

Feed: <https://tinygo.org/index.xml>

- [Raspberry Pi Pico 2](https://tinygo.org/docs/reference/microcontrollers/boards/pico2)
- [Raspberry Pi Pico 2 W](https://tinygo.org/docs/reference/microcontrollers/boards/pico2-w)

### 304. 2 items; pair scores 91.4%–91.4% (mean 91.4%, 1 qualifying pairs)

Feed: <https://www.bhaskar.com/rss-feed/1061/>

- [दिल्ली बस गैंगरेप केस- 398 पेज की चार्जशीट दाखिल:सीमेन सैंपल और बाल के सबूत मिले; 4 अगस्त की घटना, ड्राइवर-कंडक्टर पकड़े गए थे](https://www.bhaskar.com/national/news/delhi-bus-gangrape-case-chargesheet-filed-semen-samples-evidence-139192751.html)
- [दिल्ली में बस में 16 साल की छात्रा से गैंगरेप:ग्रेटर नोएडा से बैठाया; बस 47km दौड़ती रही, शीशे पर पर्दे लगे थे, ड्राइवर-कंडक्टर अरेस्ट](https://www.bhaskar.com/g/national/news/delhi-sleeper-bus-gangrape-student-arrest-driver-conductor-138886323.html)

### 305. 2 items; pair scores 91.3%–91.3% (mean 91.3%, 1 qualifying pairs)

Feed: <https://forwardemail.net/blog/feed/rss>

- [✓ Investigating Outlook.com / Microsoft 365 service issues](https://github.com/forwardemail/status.forwardemail.net/issues/2507)
- [✓ Investigating Outlook.com / Microsoft 365 service issues](https://github.com/forwardemail/status.forwardemail.net/issues/2506)

### 306. 2 items; pair scores 91.3%–91.3% (mean 91.3%, 1 qualifying pairs)

Feed: <https://olvid.io/rss/fr.xml>

- [La version 4.4 d'Olvid pour macOS est disponible](https://olvid.io/download)
- [La version 4.4 d'Olvid pour iOS est disponible](https://olvid.io/download)

### 307. 2 items; pair scores 91.1%–91.1% (mean 91.1%, 1 qualifying pairs)

Feed: <https://feeds.npr.org/510313/podcast.xml>

- [Advice Line with Scott Tannen of Boll & Branch and Jamie Siminoff of Ring (2025)](https://rss.art19.com/episodes/e64c6623-10d1-4222-85d6-22eeab736221.mp3?rss_browser=bahjigtdahjvbwugogzfva%3d%3d--d05363d83ce333c74f32188013892b2863ad051c)
- [Advice Line with Scott Tannen of Boll & Branch and Jamie Siminoff of Ring](https://rss.art19.com/episodes/38d32c1e-dfc5-4c9d-b31a-446a0ea201d6.mp3?rss_browser=bahjigtdahjvbwugogzfva%3d%3d--d05363d83ce333c74f32188013892b2863ad051c)

### 308. 2 items; pair scores 91.1%–91.1% (mean 91.1%, 1 qualifying pairs)

Feed: <https://investorplace.com/content-feed/>

- [The “October Surprise” Wall Street Isn’t Ready For – and How You Can Prepare](https://investorplace.com/smartmoney/2026/09/october-surprise-wall-street-how-prepare)
- [A 92% Historical Market Event Could Begin Before Election Day](https://investorplace.com/market360/2026/09/a-92-historical-market-event-could-begin-before-election-day)

### 309. 2 items; pair scores 91.1%–91.1% (mean 91.1%, 1 qualifying pairs)

Feed: <https://forwardemail.net/blog/feed/rss>

- [✓ Investigating Outlook.com / Microsoft 365 service issues](https://github.com/forwardemail/status.forwardemail.net/issues/2504)
- [✓ Investigating Outlook.com / Microsoft 365 service issues](https://github.com/forwardemail/status.forwardemail.net/issues/2503)

### 310. 2 items; pair scores 90.9%–90.9% (mean 90.9%, 1 qualifying pairs)

Feed: <https://thewaltdisneycompany.com/feed/>

- [“Disney Celebrates America” Ramps Up With A Phenomenal Lineup Of Experiences Ahead Of The Nation’s 250th Anniversary](https://thewaltdisneycompany.com/press-releases/disney-celebrates-america-ramps-up-with-a-phenomenal-lineup-of-experiences-ahead-of-the-nations-250th-anniversary)
- [‘Disney Celebrates America’ Ramps Up with a Phenomenal Lineup of Experiences Ahead of the Nation’s 250th Anniversary](https://thewaltdisneycompany.com/news/america-250-events-lineup)

### 311. 2 items; pair scores 90.9%–90.9% (mean 90.9%, 1 qualifying pairs)

Feed: <https://startupjunkie.libsyn.com/rss>

- [141: Launching LIVSN with Andrew Gibbs-Dabney (Rebroadcast)](https://share.transistor.fm/s/95ad0aa4)
- [141: Launching LIVSN with Andrew Gibbs-Dabney](https://share.transistor.fm/s/7cc18c73)

### 312. 2 items; pair scores 90.8%–90.8% (mean 90.8%, 1 qualifying pairs)

Feed: <https://podcasts.files.bbci.co.uk/p002w557.rss>

- [Legacy Of Alan Turing - Episode Two](http://www.bbc.co.uk/programmes/p00tgvl9)
- [Legacy Of Alan Turing - Episode One](http://www.bbc.co.uk/programmes/p00t6kkg)

### 313. 2 items; pair scores 90.6%–90.6% (mean 90.6%, 1 qualifying pairs)

Feed: <https://podcasts.files.bbci.co.uk/p002w557.rss>

- [Episode 1](http://www.bbc.co.uk/programmes/p00wmgw3)
- [Scott's Legacy: Programme 1 - Antarctica](http://www.bbc.co.uk/programmes/p00qg5fl)

### 314. 2 items; pair scores 90.5%–90.5% (mean 90.5%, 1 qualifying pairs)

Feed: <https://rss.art19.com/tim-ferriss-show>

- [#73: A Chess Prodigy on Mastering Martial Arts, Chess, and Life](https://rss.art19.com/episodes/690e2309-55de-4f9b-b4d8-07b27e728e86.mp3?rss_browser=bahjigtdahjvbwugogzfva%3d%3d--d05363d83ce333c74f32188013892b2863ad051c)
- [Episode 2: Joshua Waitzkin](https://rss.art19.com/episodes/cf652463-9388-413e-a278-a108b29b8e2b.mp3?rss_browser=bahjigtdahjvbwugogzfva%3d%3d--d05363d83ce333c74f32188013892b2863ad051c)

### 315. 2 items; pair scores 90.4%–90.4% (mean 90.4%, 1 qualifying pairs)

Feed: <https://kde.org/index.xml>

- [KDE Plasma 6.8 Beta Release](https://kde.org/announcements/plasma/6/6.7.91)
- [KDE Plasma 6.8 Beta Release](https://kde.org/announcements/plasma/6/6.7.90)

### 316. 2 items; pair scores 90.4%–90.4% (mean 90.4%, 1 qualifying pairs)

Feed: <https://tinygo.org/index.xml>

- [Pimoroni Badger2040](https://tinygo.org/docs/reference/microcontrollers/boards/badger2040)
- [Pimoroni Badger2040-W](https://tinygo.org/docs/reference/microcontrollers/boards/badger2040-w)

### 317. 2 items; pair scores 90.4%–90.4% (mean 90.4%, 1 qualifying pairs)

Feed: <https://www.gathering4gardner.org/feed/>

- [Friday News Sep 04: Weekly Socials, Virtual CoM, YT Videos](https://www.gathering4gardner.org/news-2026-09-04)
- [Friday News Aug 28: Weekly Socials, Virtual CoM, YT Videos](https://www.gathering4gardner.org/news-2026-08-28)

### 318. 2 items; pair scores 90.3%–90.3% (mean 90.3%, 1 qualifying pairs)

Feed: <https://tinygo.org/index.xml>

- [Raspberry Pi Pico](https://tinygo.org/docs/reference/microcontrollers/featured/pico)
- [Raspberry Pi Pico W](https://tinygo.org/docs/reference/microcontrollers/featured/pico-w)

### 319. 2 items; pair scores 90.3%–90.3% (mean 90.3%, 1 qualifying pairs)

Feed: <https://www.latent.space/feed>

- [\[AINews\] Opus 5.5 is good at explainer videos](https://www.latent.space/p/ainews-opus-55-is-good-at-explainer)
- [\[AINews\] The Future of Latent Space](https://www.latent.space/p/ainews-the-future-of-latent-space)

### 320. 2 items; pair scores 90.3%–90.3% (mean 90.3%, 1 qualifying pairs)

Feed: <https://forwardemail.net/blog/feed/rss>

- [✓ Investigating Outlook.com / Microsoft 365 service issues](https://github.com/forwardemail/status.forwardemail.net/issues/2512)
- [✓ Investigating Outlook.com / Microsoft 365 service issues](https://github.com/forwardemail/status.forwardemail.net/issues/2511)

### 321. 2 items; pair scores 90.2%–90.2% (mean 90.2%, 1 qualifying pairs)

Feed: <https://blog.getpaint.net/feed/>

- [Paint.NET 5.1.11 is now available](https://blog.paint.net/2025/11/09/paint-net-5-1-11-is-now-available)
- [Paint.NET 5.1.10 is now available](https://blog.paint.net/2025/11/09/paint-net-5-1-10-is-now-available)

### 322. 2 items; pair scores 90.2%–90.2% (mean 90.2%, 1 qualifying pairs)

Feed: <https://blog.paint.net/feed/>

- [Paint.NET 5.1.11 is now available](https://blog.paint.net/2025/11/09/paint-net-5-1-11-is-now-available)
- [Paint.NET 5.1.10 is now available](https://blog.paint.net/2025/11/09/paint-net-5-1-10-is-now-available)

### 323. 2 items; pair scores 90.2%–90.2% (mean 90.2%, 1 qualifying pairs)

Feed: <https://www.antonsten.com/rss.xml>

- [One Question Changed Feedback for Me](https://www.antonsten.com/articles/one-question-changed-feedback-for-me)
- [How I write for design](https://www.antonsten.com/articles/how-i-write-for-design)

### 324. 2 items; pair scores 90.2%–90.2% (mean 90.2%, 1 qualifying pairs)

Feed: <https://forwardemail.net/blog/feed/rss>

- [✓ Investigating Outlook.com / Microsoft 365 service issues](https://github.com/forwardemail/status.forwardemail.net/issues/2510)
- [✓ Investigating Outlook.com / Microsoft 365 service issues](https://github.com/forwardemail/status.forwardemail.net/issues/2509)

### 325. 2 items; pair scores 90.2%–90.2% (mean 90.2%, 1 qualifying pairs)

Feed: <https://rss.beehiiv.com/feeds/nczRb4PQ6t.xml>

- [How LinkedIn uses Event Driven Architectures to Scale](https://quastor.beehiiv.com/p/how-linkedin-uses-event-driven-architectures-to-scale-6ecc)
- [How LinkedIn uses Event Driven Architectures to Scale](https://quastor.beehiiv.com/p/how-linkedin-uses-event-driven-architectures-to-scale)

### 326. 2 items; pair scores 90.1%–90.1% (mean 90.1%, 1 qualifying pairs)

Feed: <https://hollycummins.com/rss.xml>

- [The Power of LLMs in Java – Leveraging Quarkus and LangChain4j](http://hollycummins.com/llms-and-quarkus-techxchange)
- [NLJUG academy masterclass – Create Java-based AI applications with Quarkus and LangChain4j](http://hollycummins.com/langchain4j-and-quarkus-nljug)

## Fetch errors

| Feed URL | Error |
|---|---|
| <https://a11ysavvy.com/feed/> | ClientConnectorCertificateError: Cannot connect to host www.a11ysavvy.com:443 ssl:True [SSLCertVerificationError: (1, "[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: Hostname mismatch, certificate is not valid for 'www.a11ysavvy.com'. (_ssl.c:1000)")] |
| <https://arcfu.com> | ClientConnectorSSLError: Cannot connect to host arcfu.com:443 ssl:default [[SSL: TLSV1_UNRECOGNIZED_NAME] tlsv1 unrecognized name (_ssl.c:1000)] |
| <https://arcfu.com/index.xml> | ClientConnectorSSLError: Cannot connect to host arcfu.com:443 ssl:default [[SSL: TLSV1_UNRECOGNIZED_NAME] tlsv1 unrecognized name (_ssl.c:1000)] |
| <https://askitout.com/feed/> | ClientConnectorSSLError: Cannot connect to host askitout.com:443 ssl:default [[SSL: TLSV1_ALERT_INTERNAL_ERROR] tlsv1 alert internal error (_ssl.c:1000)] |
| <https://bbhtt.space/index.xml> | ClientConnectorSSLError: Cannot connect to host bbhtt.space:443 ssl:default [[SSL: TLSV1_UNRECOGNIZED_NAME] tlsv1 unrecognized name (_ssl.c:1000)] |
| <https://binaryengineacademy.com/feed/> | ClientConnectorSSLError: Cannot connect to host binaryengineacademy.com:443 ssl:default [[SSL: TLSV1_UNRECOGNIZED_NAME] tlsv1 unrecognized name (_ssl.c:1000)] |
| <https://bizsearch-asp.accelatech.com/bizasp/index.php?a=ANRS001&corpId=atc140016> | ClientConnectorSSLError: Cannot connect to host bizsearch-asp.accelatech.com:443 ssl:default [[SSL: DH_KEY_TOO_SMALL] dh key too small (_ssl.c:1000)] |
| <https://bokardo.com/feed/> | ClientConnectorCertificateError: Cannot connect to host www.bokardo.com:443 ssl:True [SSLCertVerificationError: (1, "[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: Hostname mismatch, certificate is not valid for 'www.bokardo.com'. (_ssl.c:1000)")] |
| <https://brendanhalpin.com/feed/> | ClientConnectorError: Cannot connect to host brendanhalpin.com:443 ssl:default [None] |
| <https://candyfab.org/feed/> | TimeoutError:  |
| <https://chainlesscoder.com/index.xml> | ClientConnectorSSLError: Cannot connect to host chainlesscoder.com:443 ssl:default [[SSL: TLSV1_UNRECOGNIZED_NAME] tlsv1 unrecognized name (_ssl.c:1000)] |
| <https://codeberg.org/OSI-Concerns/election-results-2025.rss> | ClientConnectorError: Cannot connect to host codeberg.org:443 ssl:default [None] |
| <https://codeberg.org/kcxt/6502.sh.rss> | ClientConnectorError: Cannot connect to host codeberg.org:443 ssl:default [None] |
| <https://codestyleandtaste.com/rss.xml> | ClientConnectorCertificateError: Cannot connect to host www.codestyleandtaste.com:443 ssl:True [SSLCertVerificationError: (1, "[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: Hostname mismatch, certificate is not valid for 'www.codestyleandtaste.com'. (_ssl.c:1000)")] |
| <https://eaglepubs.erau.edu/introductiontoaerospaceflightvehicles/feed/> | TimeoutError:  |
| <https://eviltux.com/feed/> | ClientConnectorCertificateError: Cannot connect to host www.eviltux.com:443 ssl:True [SSLCertVerificationError: (1, '[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: unable to get local issuer certificate (_ssl.c:1000)')] |
| <https://extra.ie/feed> | ServerDisconnectedError: Server disconnected |
| <https://extremq.com/feed/?type=rss> | ClientConnectorSSLError: Cannot connect to host extremq.com:443 ssl:default [[SSL: TLSV1_UNRECOGNIZED_NAME] tlsv1 unrecognized name (_ssl.c:1000)] |
| <https://feeds.akkartik.name/kartiks-scrapbook> | ClientConnectorError: Cannot connect to host feeds.akkartik.name:443 ssl:default [None] |
| <https://flak.tedunangst.com/rss> | ClientConnectorSSLError: Cannot connect to host flak.tedunangst.com:443 ssl:default [[SSL: TLSV1_UNRECOGNIZED_NAME] tlsv1 unrecognized name (_ssl.c:1000)] |
| <https://forexpolicy.com/feed/> | ClientConnectorCertificateError: Cannot connect to host www.forexpolicy.com:443 ssl:True [SSLCertVerificationError: (1, '[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: certificate has expired (_ssl.c:1000)')] |
| <https://garagedreams.net/feed> | ClientConnectorError: Cannot connect to host garagedreams.net:443 ssl:default [None] |
| <https://globalbulletin24.com/feed/> | ClientConnectorSSLError: Cannot connect to host globalbulletin24.com:443 ssl:default [[SSL: TLSV1_UNRECOGNIZED_NAME] tlsv1 unrecognized name (_ssl.c:1000)] |
| <https://heritage-centre.co.uk/feed/> | ClientConnectorCertificateError: Cannot connect to host www.heritage-centre.co.uk:443 ssl:True [SSLCertVerificationError: (1, '[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: self-signed certificate (_ssl.c:1000)')] |
| <https://investigatorypowerstribunal.org.uk/feed/> | ClientConnectorError: Cannot connect to host investigatorypowerstribunal.org.uk:443 ssl:default [None] |
| <https://joebew42.github.io /feed.xml> | ClientConnectorDNSError: Cannot connect to host joebew42.github.io :443 ssl:default [Name or service not known] |
| <https://johnhawks.net/feed> | ClientConnectorError: Cannot connect to host johnhawks.net:443 ssl:default [None] |
| <https://journalijsra.com/rss.xml> | ClientConnectorCertificateError: Cannot connect to host www.journalijsra.com:443 ssl:True [SSLCertVerificationError: (1, '[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: self-signed certificate (_ssl.c:1000)')] |
| <https://justine.lol/rss.xml> | ClientConnectorCertificateError: Cannot connect to host www.justine.lol:443 ssl:True [SSLCertVerificationError: (1, "[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: Hostname mismatch, certificate is not valid for 'www.justine.lol'. (_ssl.c:1000)")] |
| <https://kbdumps.com/feed/> | ClientConnectorSSLError: Cannot connect to host kbdumps.com:443 ssl:default [[SSL: TLSV1_UNRECOGNIZED_NAME] tlsv1 unrecognized name (_ssl.c:1000)] |
| <https://kentnerburn.com/feed/> | ClientConnectorCertificateError: Cannot connect to host www.kentnerburnauthor.com:443 ssl:True [SSLCertVerificationError: (1, '[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: unable to get local issuer certificate (_ssl.c:1000)')] |
| <https://kerkour.com/feed.xml> | ClientConnectorError: Cannot connect to host kerkour.com:443 ssl:default [None] |
| <https://kiranet.org/index.xml> | TimeoutError:  |
| <https://laughingsquid.com/feed/> | ClientConnectorError: Cannot connect to host laughingsquid.com:443 ssl:default [None] |
| <https://lawsofux.com/index.xml> | ClientOSError: [Errno 32] Broken pipe |
| <https://libertystreeteconomics.newyorkfed.org/feed/> | ClientOSError: [Errno 32] Broken pipe |
| <https://library.ulisp.com/rss?2S7B+3> | ClientConnectorError: Cannot connect to host library.ulisp.com:443 ssl:default [Connect call failed ('80.248.178.40', 443)] |
| <https://librearts.org/index.xml> | ClientConnectorError: Cannot connect to host librearts.org:443 ssl:default [Connect call failed ('65.108.31.59', 443)] |
| <https://livesys.se/index.xml> | ClientConnectorSSLError: Cannot connect to host livesys.se:443 ssl:default [[SSL: TLSV1_UNRECOGNIZED_NAME] tlsv1 unrecognized name (_ssl.c:1000)] |
| <https://maliyyat.com/feed/> | ClientConnectorSSLError: Cannot connect to host maliyyat.com:443 ssl:default [[SSL: TLSV1_UNRECOGNIZED_NAME] tlsv1 unrecognized name (_ssl.c:1000)] |
| <https://miamijournals.com/feed/> | ClientConnectorSSLError: Cannot connect to host miamijournals.com:443 ssl:default [[SSL: TLSV1_UNRECOGNIZED_NAME] tlsv1 unrecognized name (_ssl.c:1000)] |
| <https://minener.com/feed/> | ClientConnectorSSLError: Cannot connect to host minener.com:443 ssl:default [[SSL: TLSV1_ALERT_INTERNAL_ERROR] tlsv1 alert internal error (_ssl.c:1000)] |
| <https://monero.forex/feed/> | ClientConnectorSSLError: Cannot connect to host monero.forex:443 ssl:default [[SSL: TLSV1_UNRECOGNIZED_NAME] tlsv1 unrecognized name (_ssl.c:1000)] |
| <https://mrsteinberg.com/feed/> | ClientConnectorSSLError: Cannot connect to host mrsteinberg.com:443 ssl:default [[SSL: TLSV1_UNRECOGNIZED_NAME] tlsv1 unrecognized name (_ssl.c:1000)] |
| <https://mwi.westpoint.edu/feed/> | ClientConnectorError: Cannot connect to host mwi.westpoint.edu:443 ssl:default [None] |
| <https://nautil.us/feed/> | ClientConnectorError: Cannot connect to host nautil.us:443 ssl:default [None] |
| <https://news.sophos.com/feed/> | TimeoutError:  |
| <https://ngrok.com/blog-post/rss.xml> | ClientOSError: [Errno 32] Broken pipe |
| <https://oldermuscles.com/feed/> | TimeoutError:  |
| <https://onsmalltalk.com/seaside/rssFeed> | ClientConnectorError: Cannot connect to host onsmalltalk.com:443 ssl:default [Connect call failed ('173.255.196.132', 443)] |
| <https://petebachant.me /feed.xml> | ClientConnectorDNSError: Cannot connect to host petebachant.me :443 ssl:default [Name or service not known] |
| <https://pracap.com/feed/> | ClientConnectorCertificateError: Cannot connect to host www.pracap.com:443 ssl:True [SSLCertVerificationError: (1, '[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: unable to get local issuer certificate (_ssl.c:1000)')] |
| <https://propertybuy-rent.com/feed/> | TimeoutError:  |
| <https://rebruit.com/feed/> | ClientConnectorCertificateError: Cannot connect to host www.rebruit.com:443 ssl:True [SSLCertVerificationError: (1, "[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: Hostname mismatch, certificate is not valid for 'www.rebruit.com'. (_ssl.c:1000)")] |
| <https://reinventingtheweb.com/feed/> | ClientConnectorSSLError: Cannot connect to host reinventingtheweb.com:443 ssl:default [[SSL: TLSV1_UNRECOGNIZED_NAME] tlsv1 unrecognized name (_ssl.c:1000)] |
| <https://securityintelligence.com/feed/> | ClientConnectorCertificateError: Cannot connect to host www.securityintelligence.com:443 ssl:True [SSLCertVerificationError: (1, '[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: unable to get local issuer certificate (_ssl.c:1000)')] |
| <https://selfishsoftware.com/feed> | ClientConnectorSSLError: Cannot connect to host selfishsoftware.com:443 ssl:default [[SSL: SSLV3_ALERT_HANDSHAKE_FAILURE] sslv3 alert handshake failure (_ssl.c:1000)] |
| <https://shekhargulati.com/feed/> | ClientConnectorCertificateError: Cannot connect to host www.shekhargulati.com:443 ssl:True [SSLCertVerificationError: (1, "[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: Hostname mismatch, certificate is not valid for 'www.shekhargulati.com'. (_ssl.c:1000)")] |
| <https://siril.org/index.xml> | ClientConnectorError: Cannot connect to host siril.org:443 ssl:default [None] |
| <https://sizeof.cat/index.xml> | ClientConnectorSSLError: Cannot connect to host sizeof.cat:443 ssl:default [[SSL: TLSV1_ALERT_INTERNAL_ERROR] tlsv1 alert internal error (_ssl.c:1000)] |
| <https://spaceexplored.com/feed> | ClientConnectorError: Cannot connect to host spaceexplored.com:443 ssl:default [None] |
| <https://startupsanonymous.com/feed/> | TimeoutError:  |
| <https://stubx.info/feed/> | TimeoutError:  |
| <https://sumanthrh.com/index.xml> | ClientOSError: [Errno 32] Broken pipe |
| <https://swarm.ptsecurity.com/feed/> | ClientConnectorSSLError: Cannot connect to host www.swarm.ptsecurity.com:443 ssl:default [[SSL: TLSV1_UNRECOGNIZED_NAME] tlsv1 unrecognized name (_ssl.c:1000)] |
| <https://techripoti.com/feed/> | ClientConnectorCertificateError: Cannot connect to host www.techripoti.com:443 ssl:True [SSLCertVerificationError: (1, '[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: certificate has expired (_ssl.c:1000)')] |
| <https://thedubaiweekly.com/feed/> | ClientConnectorSSLError: Cannot connect to host thedubaiweekly.com:443 ssl:default [[SSL: TLSV1_UNRECOGNIZED_NAME] tlsv1 unrecognized name (_ssl.c:1000)] |
| <https://thenigerialawyer.com/feed/> | ClientConnectorError: Cannot connect to host thenigerialawyer.com:443 ssl:default [None] |
| <https://theonion.com/feed/> | ClientConnectorError: Cannot connect to host theonion.com:443 ssl:default [None] |
| <https://urtext.co/feed/> | ClientConnectorSSLError: Cannot connect to host urtext.co:443 ssl:default [[SSL: TLSV1_UNRECOGNIZED_NAME] tlsv1 unrecognized name (_ssl.c:1000)] |
| <https://wapp.tcl.tk/home/timeline.rss> | ClientConnectorSSLError: Cannot connect to host wapp.tcl.tk:443 ssl:default [[SSL: TLSV1_UNRECOGNIZED_NAME] tlsv1 unrecognized name (_ssl.c:1000)] |
| <https://weeklyinsights.co.uk/feed/> | ClientConnectorSSLError: Cannot connect to host weeklyinsights.co.uk:443 ssl:default [[SSL: TLSV1_UNRECOGNIZED_NAME] tlsv1 unrecognized name (_ssl.c:1000)] |
| <https://www.billboard.com/feed/rss/> | ClientOSError: [Errno 32] Broken pipe |
| <https://www.bluephoto.biz/feed/> | ClientConnectorSSLError: Cannot connect to host www.bluephoto.biz:443 ssl:default [[SSL: TLSV1_UNRECOGNIZED_NAME] tlsv1 unrecognized name (_ssl.c:1000)] |
| <https://www.bookofjoe.com/index.rdf> | ClientConnectorCertificateError: Cannot connect to host www.bookofjoe.com:443 ssl:True [SSLCertVerificationError: (1, "[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: Hostname mismatch, certificate is not valid for 'www.bookofjoe.com'. (_ssl.c:1000)")] |
| <https://www.buzzhint.com/feed/> | ClientConnectorCertificateError: Cannot connect to host www.buzzhint.com:443 ssl:True [SSLCertVerificationError: (1, '[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: unable to get local issuer certificate (_ssl.c:1000)')] |
| <https://www.cellcrypt.co.uk/blog-feed.xml> | ClientConnectorSSLError: Cannot connect to host www.cellcrypt.co.uk:443 ssl:default [[SSL: TLSV1_UNRECOGNIZED_NAME] tlsv1 unrecognized name (_ssl.c:1000)] |
| <https://www.dailydot.com/feed/> | ClientOSError: [Errno 32] Broken pipe |
| <https://www.djmentors.com/feed> | TimeoutError:  |
| <https://www.doobybrain.com/blog?format=rss> | TimeoutError:  |
| <https://www.ericbutton.co/feed> | TimeoutError:  |
| <https://www.ericdaigle.ca/index.xml> | ClientConnectorSSLError: Cannot connect to host www.ericdaigle.ca:443 ssl:default [[SSL: TLSV1_UNRECOGNIZED_NAME] tlsv1 unrecognized name (_ssl.c:1000)] |
| <https://www.happiness.hks.harvard.edu/february-2025-issue?format=rss> | ClientConnectorCertificateError: Cannot connect to host www.happiness.hks.harvard.edu:443 ssl:True [SSLCertVerificationError: (1, "[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: Hostname mismatch, certificate is not valid for 'www.happiness.hks.harvard.edu'. (_ssl.c:1000)")] |
| <https://www.ic3.gov/PSA/rss> | ClientOSError: [Errno 32] Broken pipe |
| <https://www.keenformatics.com/feed.xml> | ClientOSError: [Errno 32] Broken pipe |
| <https://www.kentik.com/feed.xml> | ClientOSError: [Errno 32] Broken pipe |
| <https://www.kimiascience.com/index.php?format=feed&type=rss> | ClientConnectorSSLError: Cannot connect to host www.kimiascience.com:443 ssl:default [[SSL: TLSV1_UNRECOGNIZED_NAME] tlsv1 unrecognized name (_ssl.c:1000)] |
| <https://www.kith.org/feed/> | TimeoutError:  |
| <https://www.kookbooknook.com/posts.rss> | ClientConnectorSSLError: Cannot connect to host www.kookbooknook.com:443 ssl:default [[SSL: TLSV1_UNRECOGNIZED_NAME] tlsv1 unrecognized name (_ssl.c:1000)] |
| <https://www.lightbluetouchpaper.org/feed/> | ClientConnectorCertificateError: Cannot connect to host www.lightbluetouchpaper.org:443 ssl:True [SSLCertVerificationError: (1, '[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: certificate has expired (_ssl.c:1000)')] |
| <https://www.melbournewire.com/feed/> | ClientConnectorSSLError: Cannot connect to host www.melbournewire.com:443 ssl:default [[SSL: TLSV1_UNRECOGNIZED_NAME] tlsv1 unrecognized name (_ssl.c:1000)] |
| <https://www.motherjones.com/feed/> | ClientConnectorError: Cannot connect to host www.motherjones.com:443 ssl:default [None] |
| <https://www.muskwatch.com/feed> | ClientConnectorSSLError: Cannot connect to host www.muskwatch.com:443 ssl:default [[SSL: SSLV3_ALERT_HANDSHAKE_FAILURE] sslv3 alert handshake failure (_ssl.c:1000)] |
| <https://www.orsolabs.com/index.xml> | ClientOSError: [Errno 32] Broken pipe |
| <https://www.piecesandperiods.com/feed> | ClientConnectorSSLError: Cannot connect to host www.piecesandperiods.com:443 ssl:default [[SSL: SSLV3_ALERT_HANDSHAKE_FAILURE] sslv3 alert handshake failure (_ssl.c:1000)] |
| <https://www.scrapstostacks.com/blog-feed.xml> | TooManyRedirects: 0, message='', url='https://www.scrapstostacks.com/blog-feed.xml' |
| <https://www.seangoedecke.com/rss.xml> | ClientOSError: [Errno 32] Broken pipe |
| <https://www.techentfut.com/rss.xml> | ClientConnectorSSLError: Cannot connect to host www.techentfut.com:443 ssl:default [[SSL: TLSV1_UNRECOGNIZED_NAME] tlsv1 unrecognized name (_ssl.c:1000)] |
| <https://www.technoblogy.com/rss?KVM+3> | ClientConnectorError: Cannot connect to host www.technoblogy.com:443 ssl:default [Connect call failed ('80.248.178.40', 443)] |
| <https://www.thephcheese.com/feed> | ClientConnectorSSLError: Cannot connect to host www.thephcheese.com:443 ssl:default [[SSL: TLSV1_UNRECOGNIZED_NAME] tlsv1 unrecognized name (_ssl.c:1000)] |
| <https://www.thezimbabwemail.com/feed/> | ClientConnectorSSLError: Cannot connect to host www.thezimbabwemail.com:443 ssl:default [[SSL: SSLV3_ALERT_HANDSHAKE_FAILURE] sslv3 alert handshake failure (_ssl.c:1000)] |
| <https://www.twz.com/feed> | ClientConnectorError: Cannot connect to host www.twz.com:443 ssl:default [None] |
| <https://www.ulisp.com/rss?3J+3> | ClientConnectorError: Cannot connect to host www.ulisp.com:443 ssl:default [Connect call failed ('80.248.178.40', 443)] |
| <https://www.wecb.xyz/feed/> | ClientConnectorSSLError: Cannot connect to host www.wecb.xyz:443 ssl:default [[SSL: TLSV1_UNRECOGNIZED_NAME] tlsv1 unrecognized name (_ssl.c:1000)] |
| <https://www.worldweatherattribution.org/feed/> | ClientPayloadError: Response payload is not completed: <TransferEncodingError: 400, message='Not enough data to satisfy transfer length header.'>. ConnectionResetError(104, 'Connection reset by peer') |
| <https://www.zdnet.com/topic/artificial-intelligence/rss.xml> | ClientConnectorError: Cannot connect to host www.zdnet.com:443 ssl:default [None] |
| <http://consequenceofsound.net/feed> | ClientConnectorError: Cannot connect to host consequenceofsound.net:443 ssl:default [None] |
| <http://thegrowthshow.hubspot.libsynpro.com/> | ServerDisconnectedError: Server disconnected |
| <http://www.historynet.com/feed> | ClientOSError: [Errno 32] Broken pipe |
| <https://androidcommunity.com/feed/> | ClientConnectorError: Cannot connect to host androidcommunity.com:443 ssl:default [Connect call failed ('147.182.201.119', 443)] |
| <https://architizer.wpengine.com/feed/> | TimeoutError:  |
| <https://completedeveloperpodcast.com/feed/podcast/> | ClientConnectorSSLError: Cannot connect to host completedeveloperpodcast.com:443 ssl:default [[SSL: TLSV1_UNRECOGNIZED_NAME] tlsv1 unrecognized name (_ssl.c:1000)] |
| <https://feeds.feedwrench.com/all-shows-devchattv.rss> | TimeoutError:  |
| <https://feeds.folha.uol.com.br/emcimadahora/rss091.xml> | UnicodeDecodeError: 'utf-8' codec can't decode byte 0xed in position 387: invalid continuation byte |
| <https://film.avclub.com/rss> | ClientConnectorSSLError: Cannot connect to host film.avclub.com:443 ssl:default [[SSL: TLSV1_UNRECOGNIZED_NAME] tlsv1 unrecognized name (_ssl.c:1000)] |
| <https://jakewharton.com/atom.xml> | ClientConnectorError: Cannot connect to host jakewharton.com:443 ssl:default [None] |
| <https://rss.whooshkaa.com/rss/podcast/id/1308> | ClientConnectorSSLError: Cannot connect to host rss.whooshkaa.com:443 ssl:default [[SSL: TLSV1_UNRECOGNIZED_NAME] tlsv1 unrecognized name (_ssl.c:1000)] |
| <https://slack.engineering/feed> | ClientConnectorError: Cannot connect to host slack.engineering:443 ssl:default [None] |
| <https://stuckincustoms.com/feed/> | ClientConnectorError: Cannot connect to host stuckincustoms.com:443 ssl:default [None] |
| <https://www.elnorte.com/rss/portada.xml> | UnicodeDecodeError: 'utf-8' codec can't decode byte 0xfa in position 216: invalid start byte |
| <https://www.macworld.com/index.rss> | ClientConnectorError: Cannot connect to host www.macworld.com:443 ssl:default [None] |
| <https://www.reforma.com/rss/portada.xml> | UnicodeDecodeError: 'utf-8' codec can't decode byte 0xfa in position 216: invalid start byte |
| <https://www.savingadvice.com/feed/> | ClientConnectorError: Cannot connect to host www.savingadvice.com:443 ssl:default [None] |
| <https://techcrunch.com/category/startups/feed/> | ClientConnectorError: Cannot connect to host techcrunch.com:443 ssl:default [None] |
| <https://wecanmag.com/feed/> | TimeoutError:  |
| <https://thestartuppitch.com/feed/> | ClientConnectorError: Cannot connect to host thestartuppitch.com:443 ssl:default [None] |
| <https://500hats.com/feed> | TimeoutError:  |
| <https://www.starteer.com/feed/> | ClientConnectorError: Cannot connect to host www.starteer.com:443 ssl:default [None] |
| <https://clearstrategyco.com/feed/> | TimeoutError:  |
| <https://www.webuyitgreen.com/feed/> | TimeoutError:  |
| <https://insidehpc.com/feed/> | ClientConnectorSSLError: Cannot connect to host insidehpc.com:443 ssl:default [[SSL: SSLV3_ALERT_HANDSHAKE_FAILURE] sslv3 alert handshake failure (_ssl.c:1000)] |
| <https://www.michaelbromley.co.uk/index.xml> | ClientOSError: [Errno 32] Broken pipe |
| <https://bennorthmore.com/rss.xml> | ClientOSError: [Errno 32] Broken pipe |
| <https://www.hypertalking.com/feed/> | ClientConnectorCertificateError: Cannot connect to host www.hypertalking.com:443 ssl:True [SSLCertVerificationError: (1, "[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: Hostname mismatch, certificate is not valid for 'www.hypertalking.com'. (_ssl.c:1000)")] |
| <https://insoniaoculta.com.br/feed> | ClientConnectorSSLError: Cannot connect to host insoniaoculta.com.br:443 ssl:default [[SSL: TLSV1_UNRECOGNIZED_NAME] tlsv1 unrecognized name (_ssl.c:1000)] |
| <https://sekhmetdesign.thegeekcartel.com/feed/> | ClientConnectorCertificateError: Cannot connect to host www.sekhmetdesign.thegeekcartel.com:443 ssl:True [SSLCertVerificationError: (1, '[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: certificate has expired (_ssl.c:1000)')] |
| <https://fujinet.online/feed/> | ClientConnectorError: Cannot connect to host fujinet.online:443 ssl:default [Connect call failed ('104.248.6.55', 443)] |
| <http://localhost:1313/index.xml> | ClientConnectorError: Cannot connect to host localhost:1313 ssl:default [Connect call failed ('127.0.0.1', 1313)] |
| <https://trevmex.com/rss> | ClientConnectorError: Cannot connect to host trevmex.com:443 ssl:default [None] |
| <https://zinzy.website//index.xml> | ClientConnectorError: Cannot connect to host zinzy.website:443 ssl:default [None] |
| <https://talkerresearch.com/feed/> | ServerDisconnectedError: Server disconnected |
| <https://www.zlhgo.com/index.xml> | TimeoutError:  |
| <https://www.boldfaceline.com/feed.xml> | ClientConnectorError: Cannot connect to host www.boldfaceline.com:443 ssl:default [Connect call failed ('65.108.84.33', 443)] |
| <https://felaktig.info/feed/> | ClientConnectorError: Cannot connect to host felaktig.info:443 ssl:default [None] |
| <https://terraforminglatam.net/feed/> | ClientConnectorError: Cannot connect to host terraforminglatam.net:443 ssl:default [None] |
| <https://dallincrump.com/feed/> | ClientConnectorError: Cannot connect to host dallincrump.com:443 ssl:default [None] |
| <https://stereogum.com/feed> | ClientConnectorError: Cannot connect to host stereogum.com:443 ssl:default [None] |
| <https://jacobtomlinson.dev/feed.xml> | ClientConnectorError: Cannot connect to host jacobtomlinson.dev:443 ssl:default [None] |
| <https://www.mistakesweremade.xyz/rss/> | TimeoutError:  |
| <https://clickhouse.com/rss.xml> | ClientResponseError: 400, message='Got more than 8190 bytes when reading: b"default-src \'self\' https://www.googletagmanager.com; media-src \'self\' https://clickhouse.com; script...".', url='https://clickhouse.com/rss.xml' |
| <https://mackenzieinstitute.com/feed/> | ClientConnectorCertificateError: Cannot connect to host www.mackenzieinstitute.com:443 ssl:True [SSLCertVerificationError: (1, "[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: Hostname mismatch, certificate is not valid for 'www.mackenzieinstitute.com'. (_ssl.c:1000)")] |
| <https://www.mreinfo.com/feed/> | ClientConnectorCertificateError: Cannot connect to host www.mreinfo.com:443 ssl:True [SSLCertVerificationError: (1, '[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: unable to get local issuer certificate (_ssl.c:1000)')] |
| <https://flightodyssey.com/feed/> | TooManyRedirects: 0, message='', url='https://flightodyssey.com/feed/' |
| <https://www.savvycanary.com/rss/> | ClientConnectorCertificateError: Cannot connect to host www.savvycanary.com:443 ssl:True [SSLCertVerificationError: (1, "[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: Hostname mismatch, certificate is not valid for 'www.savvycanary.com'. (_ssl.c:1000)")] |
| <https://vixen.moe/rss/> | ClientConnectorSSLError: Cannot connect to host vixen.moe:443 ssl:default [[SSL: TLSV1_UNRECOGNIZED_NAME] tlsv1 unrecognized name (_ssl.c:1000)] |
| <https://calcfi.app/feed.xml> | TimeoutError:  |
| <https://www.dudewhereisthiscar.com/feed/> | ClientConnectorSSLError: Cannot connect to host www.dudewhereisthiscar.com:443 ssl:default [[SSL: TLSV1_UNRECOGNIZED_NAME] tlsv1 unrecognized name (_ssl.c:1000)] |
| <https://heyrebel.world/feed/> | ClientConnectorSSLError: Cannot connect to host heyrebel.world:443 ssl:default [[SSL: TLSV1_UNRECOGNIZED_NAME] tlsv1 unrecognized name (_ssl.c:1000)] |
| <https://kbalt.com/feed/> | TimeoutError:  |
| <https://jtwoodhouse.com/feed/?type=rss> | ClientConnectorSSLError: Cannot connect to host jtwoodhouse.com:443 ssl:default [[SSL: TLSV1_UNRECOGNIZED_NAME] tlsv1 unrecognized name (_ssl.c:1000)] |
| <https://schneider-ki.com/feed/> | ClientConnectorError: Cannot connect to host schneider-ki.com:443 ssl:default [None] |
| <https://shortspan.ai/feed.xml> | ClientConnectorError: Cannot connect to host shortspan.ai:443 ssl:default [Network is unreachable] |
| <https://www.simplymacro.xyz/posts/rss/> | ClientConnectorCertificateError: Cannot connect to host www.simplymacro.xyz:443 ssl:True [SSLCertVerificationError: (1, "[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: Hostname mismatch, certificate is not valid for 'www.simplymacro.xyz'. (_ssl.c:1000)")] |
| <https://www.solarpaces.org/feed/> | ClientConnectorError: Cannot connect to host www.solarpaces.org:443 ssl:default [None] |
| <https://feeds.thefishsite.com/thefishsite-all> | ClientConnectorSSLError: Cannot connect to host feeds.thefishsite.com:443 ssl:default [[SSL: TLSV1_UNRECOGNIZED_NAME] tlsv1 unrecognized name (_ssl.c:1000)] |
| <https://polyphon.ai/index.xml> | ClientConnectorSSLError: Cannot connect to host polyphon.ai:443 ssl:default [[SSL: TLSV1_UNRECOGNIZED_NAME] tlsv1 unrecognized name (_ssl.c:1000)] |
| <https://securitycryptographywhatever.com/feed.xml> | ClientConnectorError: Cannot connect to host securitycryptographywhatever.com:443 ssl:default [None] |
| <https://www.techsenser.com/feed/> | TooManyRedirects: 0, message='', url='https://www.techsenser.com/feed/' |
| <https://narravista.com.br/feed/> | ClientConnectorSSLError: Cannot connect to host narravista.com.br:443 ssl:default [[SSL: TLSV1_ALERT_INTERNAL_ERROR] tlsv1 alert internal error (_ssl.c:1000)] |
| <https://www.learnwithwebstories.com/feeds/posts/default?alt=rss> | ClientConnectorSSLError: Cannot connect to host www.learnwithwebstories.com:443 ssl:default [[SSL: TLSV1_UNRECOGNIZED_NAME] tlsv1 unrecognized name (_ssl.c:1000)] |
| <https://www.insidevoice.ai/feed> | ClientConnectorError: Cannot connect to host insidevoice.ai:443 ssl:default [None] |
| <https://harmonique.one/posts.rss> | ClientConnectorError: Cannot connect to host harmonique.one:443 ssl:default [None] |
| <https://www.gamespark.jp/rss20/index.rdf> | ClientConnectionResetError: Cannot write to closing transport |
| <https://www.ragman.net/rss.xml> | ClientConnectorError: Cannot connect to host www.ragman.net:443 ssl:default [None] |
| <https://getsops.io/index.xml> | ServerDisconnectedError: Server disconnected |
| <https://abeykoshyitty.com/feed/> | ClientConnectorError: Cannot connect to host abeykoshyitty.com:443 ssl:default [None] |
| <https://wittorf.works/feed> | ClientConnectorSSLError: Cannot connect to host wittorf.works:443 ssl:default [[SSL: TLSV1_UNRECOGNIZED_NAME] tlsv1 unrecognized name (_ssl.c:1000)] |
| <https://sanfranciscodownload.com/rss.xml> | ClientConnectorSSLError: Cannot connect to host sanfranciscodownload.com:443 ssl:default [[SSL: TLSV1_UNRECOGNIZED_NAME] tlsv1 unrecognized name (_ssl.c:1000)] |
| <https://chrisbergeron.com/rss2.xml> | ClientConnectorError: Cannot connect to host chrisbergeron.com:443 ssl:default [None] |
| <https://clawtrak.com/feed.xml> | ClientConnectorSSLError: Cannot connect to host clawtrak.com:443 ssl:default [[SSL: TLSV1_UNRECOGNIZED_NAME] tlsv1 unrecognized name (_ssl.c:1000)] |
| <https://agenticdev.blog/feed.xml> | ClientConnectorSSLError: Cannot connect to host agenticdev.blog:443 ssl:default [[SSL: SSLV3_ALERT_HANDSHAKE_FAILURE] sslv3 alert handshake failure (_ssl.c:1000)] |
| <https://rumproarious.com/index.xml> | ClientConnectorError: Cannot connect to host rumproarious.com:443 ssl:default [None] |
| <https://www.akitaonrails.com/index.xml> | ClientOSError: [Errno 32] Broken pipe |
| <https://wsvn.com/feed/> | ClientOSError: [Errno 32] Broken pipe |
| <https://tereza.dev/feed.xml> | ClientConnectorSSLError: Cannot connect to host tereza.dev:443 ssl:default [[SSL: TLSV1_UNRECOGNIZED_NAME] tlsv1 unrecognized name (_ssl.c:1000)] |
| <https://diegolopez.dev/feed/> | TimeoutError:  |
| <https://www.cantika.com/rss> | TimeoutError:  |
| <https://www.zilog.com/index.php?format=feed&type=rss> | ClientPayloadError: 400, message:
  Can not decode content-encoding: gzip |
| <https://outofbound.net/index.xml> | ClientConnectorError: Cannot connect to host outofbound.net:443 ssl:default [Connect call failed ('172.232.14.95', 443)] |
| <https://www.theamericanletter.com/feeds/posts/default?alt=rss> | ClientConnectorError: Cannot connect to host www.theamericanletter.com:443 ssl:default [None] |
| <https://fractalisme.nl/rss.xml> | ClientConnectorSSLError: Cannot connect to host fractalisme.nl:443 ssl:default [[SSL: TLSV1_UNRECOGNIZED_NAME] tlsv1 unrecognized name (_ssl.c:1000)] |
| <https://www.loomfeed.com/feed.xml> | ClientConnectorSSLError: Cannot connect to host www.loomfeed.com:443 ssl:default [[SSL: TLSV1_UNRECOGNIZED_NAME] tlsv1 unrecognized name (_ssl.c:1000)] |
| <https://goobar.io/feed/> | ClientConnectorSSLError: Cannot connect to host goobar.io:443 ssl:default [[SSL: TLSV1_UNRECOGNIZED_NAME] tlsv1 unrecognized name (_ssl.c:1000)] |
| <https://www.ferner-alsdorf.com/feed/> | ClientConnectorCertificateError: Cannot connect to host www.ferner-alsdorf.com:443 ssl:True [SSLCertVerificationError: (1, "[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: Hostname mismatch, certificate is not valid for 'www.ferner-alsdorf.com'. (_ssl.c:1000)")] |
| <https://neiro.it/rss.xml> | ClientConnectorError: Cannot connect to host www.neiro.it:443 ssl:default [None] |
| <https://newoldweb.com/feed> | ClientOSError: [Errno 32] Broken pipe |
| <https://www.outdoorlife.com/feed/> | ClientOSError: [Errno 32] Broken pipe |
| <https://fffff.at/feed/> | TimeoutError:  |
| <https://www.steaktek.com/feed/> | ClientConnectorCertificateError: Cannot connect to host www.steaktek.com:443 ssl:True [SSLCertVerificationError: (1, "[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: Hostname mismatch, certificate is not valid for 'www.steaktek.com'. (_ssl.c:1000)")] |
| <https://thecon.ai/feed/> | TimeoutError:  |
| <https://www.hajjibaba.org/feed/> | ServerDisconnectedError: Server disconnected |
| <https://kelly-kintner.writeas.com/feed/> | ClientConnectorError: Cannot connect to host kelly-kintner.writeas.com:443 ssl:default [None] |
| <https://chrisistrying.com/feed/> | ClientConnectorError: Cannot connect to host chrisistrying.com:443 ssl:default [None] |
| <https://www.winnipegfreepress.com/feed> | ClientConnectorError: Cannot connect to host www.winnipegfreepress.com:443 ssl:default [None] |
| <https://liquidninja.com/feed/> | TimeoutError:  |
| <https://dmitrybrant.com/feed> | TimeoutError:  |
| <https://www.aaron-gray.com/feed/> | TimeoutError:  |
| <https://cavallette.noblogs.org/feed> | TimeoutError:  |
| <https://engineering.vega-alts.com/feed> | ClientConnectorSSLError: Cannot connect to host engineering.vega-alts.com:443 ssl:default [[SSL: SSLV3_ALERT_HANDSHAKE_FAILURE] sslv3 alert handshake failure (_ssl.c:1000)] |
| <https://planetmainframe.com/feed/> | TimeoutError:  |
| <https://www.boristhebrave.com/feed/> | TimeoutError:  |
| <https://www.itamarnovick.com/feed/> | TimeoutError:  |
| <https://www.bajura.online/feeds/posts/default?alt=rss> | ClientConnectorSSLError: Cannot connect to host www.bajura.online:443 ssl:default [[SSL: TLSV1_UNRECOGNIZED_NAME] tlsv1 unrecognized name (_ssl.c:1000)] |
| <https://actuallyfreeonlinetools.com/api/feed.xml> | ClientConnectorSSLError: Cannot connect to host actuallyfreeonlinetools.com:443 ssl:default [[SSL: TLSV1_UNRECOGNIZED_NAME] tlsv1 unrecognized name (_ssl.c:1000)] |
| <https://webdesignerexperts.writeas.com/feed/> | ClientOSError: [Errno 32] Broken pipe |
| <https://le-guide-du-barbecue-et-du-four-a-bois.writeas.com/feed/> | ClientConnectorError: Cannot connect to host le-guide-du-barbecue-et-du-four-a-bois.writeas.com:443 ssl:default [None] |
| <https://www.gamesthatwerent.com/feed/> | TimeoutError:  |
| <https://boxesandarrows.com/feed/> | TimeoutError:  |
| <https://thezimbabwemail.com/feed/> | ClientConnectorSSLError: Cannot connect to host thezimbabwemail.com:443 ssl:default [[SSL: SSLV3_ALERT_HANDSHAKE_FAILURE] sslv3 alert handshake failure (_ssl.c:1000)] |
| <https://refactoringenglish.com/index.xml> | ClientConnectorError: Cannot connect to host refactoringenglish.com:443 ssl:default [None] |
| <https://theprovince.com/feed/atom> | ClientConnectorError: Cannot connect to host theprovince.com:443 ssl:default [None] |
| <https://fedcommunities.org/feed/> | ClientConnectorError: Cannot connect to host fedcommunities.org:443 ssl:default [None] |
| <https://loufranco.com/feed> | TimeoutError:  |
| <https://www.quantable.com/feed/> | ClientConnectorError: Cannot connect to host www.quantable.com:443 ssl:default [None] |
| <https://andrei.xyz/index.xml> | TimeoutError:  |
| <https://smarterarticles.co.uk/feed/> | ClientOSError: [Errno 32] Broken pipe |
| <https://attronarch.com/feed/> | ClientOSError: [Errno 32] Broken pipe |
| <https://epicmind.ch/feed/> | ClientOSError: [Errno 32] Broken pipe |
| <https://stuartbreckenridge.net/rss.xml> | ClientOSError: [Errno 32] Broken pipe |
| <http://www.fluxblog.org/feed> | TimeoutError:  |
| <https://della-wren.writeas.com/feed/> | ClientConnectorError: Cannot connect to host della-wren.writeas.com:443 ssl:default [None] |
| <https://beatrizefe.writeas.com/feed/> | ClientConnectorError: Cannot connect to host beatrizefe.writeas.com:443 ssl:default [None] |
| <https://acephale.writeas.com/feed/> | ClientConnectorError: Cannot connect to host acephale.writeas.com:443 ssl:default [None] |
| <https://www.visidata.org/feed.xml> | ClientConnectorError: Cannot connect to host www.visidata.org:443 ssl:default [None] |
| <https://sector7-signal-inkari.writeas.com/feed/> | ClientConnectorError: Cannot connect to host sector7-signal-inkari.writeas.com:443 ssl:default [None] |
| <https://gpxavier.writeas.com/feed/> | ClientPayloadError: Response payload is not completed: <TransferEncodingError: 400, message='Not enough data to satisfy transfer length header.'> |
| <https://brasilescola.uol.com.br/rss/> | ClientConnectorError: Cannot connect to host brasilescola.uol.com.br:443 ssl:default [None] |
| <https://magenta.withgoogle.com/feed.xml> | ClientConnectorError: Cannot connect to host magenta.withgoogle.com:443 ssl:default [None] |
| <https://thatnorthernbloke.writeas.com/feed/> | ClientConnectorError: Cannot connect to host thatnorthernbloke.writeas.com:443 ssl:default [None] |
| <https://fgdenton.writeas.com/feed/> | ClientConnectorError: Cannot connect to host fgdenton.writeas.com:443 ssl:default [None] |
| <https://bluseraphim.writeas.com/feed/> | ClientConnectorError: Cannot connect to host bluseraphim.writeas.com:443 ssl:default [None] |
| <https://itsphos4.writeas.com/feed/> | ClientConnectorError: Cannot connect to host itsphos4.writeas.com:443 ssl:default [None] |
| <https://discuss.linuxcontainers.org/posts.rss> | ClientConnectorError: Cannot connect to host discuss.linuxcontainers.org:443 ssl:default [None] |
| <https://thequietnotebook.writeas.com/feed/> | ClientConnectorError: Cannot connect to host thequietnotebook.writeas.com:443 ssl:default [None] |
| <https://hiddenlight.writeas.com/feed/> | ClientConnectorError: Cannot connect to host hiddenlight.writeas.com:443 ssl:default [None] |
| <https://pauladela.writeas.com/feed/> | ClientConnectorError: Cannot connect to host pauladela.writeas.com:443 ssl:default [None] |
| <https://compassionate-world.writeas.com/feed/> | ClientConnectorError: Cannot connect to host compassionate-world.writeas.com:443 ssl:default [None] |
| <https://www.sambish.com/feed.rss> | ClientOSError: [Errno 32] Broken pipe |
| <https://blattidae-paramunas.writeas.com/feed/> | ClientConnectorError: Cannot connect to host blattidae-paramunas.writeas.com:443 ssl:default [None] |
| <https://grayunashamed.writeas.com/feed/> | ClientConnectorError: Cannot connect to host grayunashamed.writeas.com:443 ssl:default [None] |
| <https://the-casual-critic.writeas.com/feed/> | ClientOSError: [Errno 32] Broken pipe |
| <https://gnostic-paradise.writeas.com/feed/> | ClientOSError: [Errno 32] Broken pipe |
| <https://rigtorp.se/index.xml> | ClientOSError: [Errno 32] Broken pipe |
| <https://www.vianegativa.us/feed/> | TimeoutError:  |
| <https://www.daniel.industries/atom.xml> | TimeoutError:  |
| <https://terrybisson.com/feed/> | TimeoutError:  |
| <https://sergemarcelroche.writeas.com/feed/> | ClientConnectorError: Cannot connect to host sergemarcelroche.writeas.com:443 ssl:default [None] |
| <https://frogtwaddle.blog/feed/> | ClientConnectorError: Cannot connect to host frogtwaddle.blog:443 ssl:default [None] |
| <https://portail-vaked-dev.pages.dev/feed.xml> | ClientConnectorSSLError: Cannot connect to host portail-vaked-dev.pages.dev:443 ssl:default [[SSL: TLSV1_UNRECOGNIZED_NAME] tlsv1 unrecognized name (_ssl.c:1000)] |
| <https://feeds.folha.uol.com.br/f5/tudo/rss091.xml> | UnicodeDecodeError: 'utf-8' codec can't decode byte 0xed in position 353: invalid continuation byte |
| <https://sslog.dpdns.org/feed.xml> | ClientConnectorSSLError: Cannot connect to host www.sslog.dpdns.org:443 ssl:default [[SSL: TLSV1_UNRECOGNIZED_NAME] tlsv1 unrecognized name (_ssl.c:1000)] |
| <https://www.scisuggest.com/feed/> | ClientConnectorSSLError: Cannot connect to host www.scisuggest.com:443 ssl:default [[SSL: TLSV1_ALERT_INTERNAL_ERROR] tlsv1 alert internal error (_ssl.c:1000)] |
| <https://gamesfromwithin.com/index.xml> | TimeoutError:  |
| <https://thebeach.dev/index.xml> | TimeoutError:  |
| <https://wondermark.com/feed/> | TimeoutError:  |
| <https://tia.mat.br/posts/rss.xml> | TimeoutError:  |
| <https://appleworld.today/feed/> | TimeoutError:  |
| <https://latinai.ch/feed/> | ClientConnectorError: Cannot connect to host latinai.ch:443 ssl:default [Connect call failed ('46.4.119.22', 443)] |
| <https://jimmysastra.com/feed/> | TimeoutError:  |
| <https://froginawell.net/frog/feed/> | TimeoutError:  |
| <https://yalibnan.com/feed/> | TimeoutError:  |
| <https://waterwatch.org/feed/> | TimeoutError:  |
| <https://qwantz.com/rssfeed.php> | TimeoutError:  |
| <https://nintendosoup.com/feed/> | TimeoutError:  |
| <https://journal.kvibber.com/feed/> | TimeoutError:  |
| <https://www.virtualtelescope.eu/feed/> | ClientConnectorCertificateError: Cannot connect to host www.virtualtelescope.eu:443 ssl:True [SSLCertVerificationError: (1, '[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: unable to get local issuer certificate (_ssl.c:1000)')] |
| <https://williamalexakis.com/feed.xml> | ClientConnectorSSLError: Cannot connect to host williamalexakis.com:443 ssl:default [[SSL: TLSV1_UNRECOGNIZED_NAME] tlsv1 unrecognized name (_ssl.c:1000)] |
| <https://www.quantumcalculus.org/feed/> | TimeoutError:  |
| <https://thearchivebase.com/feed/> | ClientConnectorSSLError: Cannot connect to host thearchivebase.com:443 ssl:default [[SSL: TLSV1_UNRECOGNIZED_NAME] tlsv1 unrecognized name (_ssl.c:1000)] |
| <https://forums.sonyinsider.com/index.php?app=core&module=global&section=rss&type=forums&id=3> | ClientConnectorSSLError: Cannot connect to host www.forums.sonyinsider.com:443 ssl:default [[SSL: TLSV1_UNRECOGNIZED_NAME] tlsv1 unrecognized name (_ssl.c:1000)] |
| <https://www.djspeckhals.com/index.xml> | ClientConnectorError: Cannot connect to host www.djspeckhals.com:443 ssl:default [None] |
| <https://earthiongame.com/feed/> | TimeoutError:  |
| <https://nome.codes/index.xml> | TimeoutError:  |
| <https://adf-magazine.com/feed/> | ClientConnectorError: Cannot connect to host adf-magazine.com:443 ssl:default [None] |
| <https://shauryaa.dev/rss.xml> | ClientConnectorSSLError: Cannot connect to host shauryaa.dev:443 ssl:default [[SSL: TLSV1_UNRECOGNIZED_NAME] tlsv1 unrecognized name (_ssl.c:1000)] |
| <https://sophiehoulden.com/feed/> | TimeoutError:  |
| <https://www.giorgiosancristoforo.net/feed/> | TimeoutError:  |
| <https://evonomics.com/feed/> | TimeoutError:  |
| <https://lucrocrm.com/feed/> | ClientConnectorCertificateError: Cannot connect to host www.lucrocrm.com:443 ssl:True [SSLCertVerificationError: (1, '[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: certificate has expired (_ssl.c:1000)')] |
| <https://eggrain.blog/feed.xml> | TimeoutError:  |
| <https://www.confuzine.com/feed/> | TimeoutError:  |
| <https://thecorner.eu/feed/> | ClientConnectorError: Cannot connect to host thecorner.eu:443 ssl:default [None] |
| <https://cliffle.com/rss.xml> | ClientConnectorError: Cannot connect to host cliffle.com:443 ssl:default [None] |
| <https://domox.tech/feed/> | ClientConnectorError: Cannot connect to host domox.tech:443 ssl:default [None] |
| <https://www.devclubhouse.com/feed> | ClientConnectorSSLError: Cannot connect to host sourcefeed.dev:443 ssl:default [[SSL: TLSV1_UNRECOGNIZED_NAME] tlsv1 unrecognized name (_ssl.c:1000)] |
| <https://01max.io/index.xml> | ClientConnectorSSLError: Cannot connect to host 01max.io:443 ssl:default [[SSL: TLSV1_UNRECOGNIZED_NAME] tlsv1 unrecognized name (_ssl.c:1000)] |
| <http://marshall.re/feed/> | ClientConnectorSSLError: Cannot connect to host marshall.re:443 ssl:default [[SSL: SSLV3_ALERT_HANDSHAKE_FAILURE] sslv3 alert handshake failure (_ssl.c:1000)] |
| <https://www.bigmessowires.com/feed/> | TimeoutError:  |
| <https://trailerparkjournal.com/feed/> | ClientConnectorSSLError: Cannot connect to host trailerparkjournal.com:443 ssl:default [[SSL: TLSV1_UNRECOGNIZED_NAME] tlsv1 unrecognized name (_ssl.c:1000)] |
| <https://shiara.antarat.com/feed/> | TimeoutError:  |
| <https://thecoinheadlines.com/feed/> | ClientConnectorSSLError: Cannot connect to host thecoinheadlines.com:443 ssl:default [[SSL: SSLV3_ALERT_HANDSHAKE_FAILURE] sslv3 alert handshake failure (_ssl.c:1000)] |
| <https://thechicagocommons.com/feed/> | ClientConnectorSSLError: Cannot connect to host thechicagocommons.com:443 ssl:default [[SSL: TLSV1_UNRECOGNIZED_NAME] tlsv1 unrecognized name (_ssl.c:1000)] |
| <https://read.lukeburgis.com/feed> | ClientConnectorSSLError: Cannot connect to host read.lukeburgis.com:443 ssl:default [[SSL: TLSV1_UNRECOGNIZED_NAME] tlsv1 unrecognized name (_ssl.c:1000)] |
| <https://sigi.ie/feed/> | ClientConnectorError: Cannot connect to host sigi.ie:443 ssl:default [None] |
| <https://www.on-sitemag.com/feed/> | ClientConnectorError: Cannot connect to host www.on-sitemag.com:443 ssl:default [Connect call failed ('50.56.2.116', 443)] |
| <https://warpedvisions.org/index.xml> | TimeoutError:  |
| <https://www.politicaexterior.com/feed/> | ServerDisconnectedError: Server disconnected |
| <https://www.typewolf.com/feed> | TimeoutError:  |
| <https://nezutero.dev/index.xml> | ClientConnectorSSLError: Cannot connect to host nezutero.dev:443 ssl:default [[SSL: TLSV1_UNRECOGNIZED_NAME] tlsv1 unrecognized name (_ssl.c:1000)] |
| <http://addyosmani.com/rss.xml> | ServerDisconnectedError: Server disconnected |
| <http://edweissman.com/rss.xml> | ClientConnectorCertificateError: Cannot connect to host www.edweissman.com:443 ssl:True [SSLCertVerificationError: (1, "[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: Hostname mismatch, certificate is not valid for 'www.edweissman.com'. (_ssl.c:1000)")] |
| <https://beej.us/blog/rss.xml> | TimeoutError:  |
| <https://blog.1password.com/index.xml> | ClientResponseError: 400, message='Got more than 8190 bytes when reading: b"default-src \'none\'; media-src \'self\' https://videos.ctfassets.net:* https://assets.qualified.com htt...".', url='https://1password.com/blog/index.xml' |
| <https://blog.davidedmundson.co.uk/feed/> | TimeoutError:  |
| <https://blog.edward-li.com/index.xml> | ClientConnectorCertificateError: Cannot connect to host www.blog.edward-li.com:443 ssl:True [SSLCertVerificationError: (1, "[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: Hostname mismatch, certificate is not valid for 'www.blog.edward-li.com'. (_ssl.c:1000)")] |
| <https://blog.fefe.de/rss.xml> | ClientConnectorError: Cannot connect to host blog.fefe.de:443 ssl:default [None] |
| <https://blog.fxn.ai/rss/> | ClientConnectorSSLError: Cannot connect to host blog.fxn.ai:443 ssl:default [[SSL: SSLV3_ALERT_HANDSHAKE_FAILURE] sslv3 alert handshake failure (_ssl.c:1000)] |
| <https://blog.gingerbeardman.com/feed.xml> | ClientOSError: [Errno 32] Broken pipe |
| <https://blog.littlepolygon.com/index.xml> | TimeoutError:  |
| <https://blog.namar0x0309.com/feed/> | TimeoutError:  |
| <https://blog.nimendra.xyz/index.xml> | ClientConnectorSSLError: Cannot connect to host blog.nimendra.xyz:443 ssl:default [[SSL: TLSV1_UNRECOGNIZED_NAME] tlsv1 unrecognized name (_ssl.c:1000)] |
| <https://blog.openint.dev/rss/> | ClientConnectorSSLError: Cannot connect to host www.blog.openint.dev:443 ssl:default [[SSL: TLSV1_UNRECOGNIZED_NAME] tlsv1 unrecognized name (_ssl.c:1000)] |
| <https://blog.piernov.org/rss/> | TimeoutError:  |
| <https://blog.run.claw.cloud/feed/> | ClientConnectorSSLError: Cannot connect to host blog.run.claw.cloud:443 ssl:default [[SSL: TLSV1_UNRECOGNIZED_NAME] tlsv1 unrecognized name (_ssl.c:1000)] |
| <https://blog.tansu.io/feed.xml> | TimeoutError:  |
| <https://blog.thenewoil.org/feed/> | TimeoutError:  |
| <https://blog.xkeeper.net/feed/> | TimeoutError:  |
| <https://colintoh.com/blog/feed> | TimeoutError:  |
| <https://dat1.co/blog/rss.xml> | ClientConnectorSSLError: Cannot connect to host dat1.co:443 ssl:default [[SSL: TLSV1_UNRECOGNIZED_NAME] tlsv1 unrecognized name (_ssl.c:1000)] |
| <https://djoker.tech/rss.xml> | ClientConnectorSSLError: Cannot connect to host djoker.tech:443 ssl:default [[SSL: TLSV1_UNRECOGNIZED_NAME] tlsv1 unrecognized name (_ssl.c:1000)] |
| <https://docs.buildwithlayer.com/blog/rss.xml> | ClientConnectorError: Cannot connect to host docs.buildwithlayer.com:443 ssl:default [None] |
| <https://docs.mcp.run/blog/rss.xml> | ClientConnectorSSLError: Cannot connect to host docs.mcp.run:443 ssl:default [[SSL: TLSV1_UNRECOGNIZED_NAME] tlsv1 unrecognized name (_ssl.c:1000)] |
| <https://emdash.io/feed/> | ClientConnectorSSLError: Cannot connect to host emdash.io:443 ssl:default [[SSL: TLSV1_UNRECOGNIZED_NAME] tlsv1 unrecognized name (_ssl.c:1000)] |
| <https://engineering.fb.com/feed/> | ClientConnectorError: Cannot connect to host engineering.fb.com:443 ssl:default [None] |
| <https://engineering.salesforce.com/feed/> | ClientConnectorError: Cannot connect to host engineering.salesforce.com:443 ssl:default [None] |
| <https://evanhahn.com/blog/index.xml> | TimeoutError:  |
| <https://fauna.com/blog/feed> | TimeoutError:  |
| <https://gaiwan.co/blog/rss/> | TimeoutError:  |
| <https://hjorthjort.github.io/feed.xml> | ClientConnectorSSLError: Cannot connect to host www.hjorthjort.xyz:443 ssl:default [[SSL: TLSV1_UNRECOGNIZED_NAME] tlsv1 unrecognized name (_ssl.c:1000)] |
| <https://ian-cooper.writeas.com/feed/> | ClientConnectorError: Cannot connect to host ian-cooper.writeas.com:443 ssl:default [None] |
| <https://lumberjack.so/feed> | ClientConnectorError: Cannot connect to host lumberjack.so:443 ssl:default [None] |
| <https://meroxa.com/blog/rss.xml> | ClientConnectorCertificateError: Cannot connect to host www.meroxa.com:443 ssl:True [SSLCertVerificationError: (1, '[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: unable to get local issuer certificate (_ssl.c:1000)')] |
| <https://mikeindustries.com/blog/feed> | TimeoutError:  |
| <https://mjtsai.com/blog/feed/> | TimeoutError:  |
| <https://nedbatchelder.com/blog/rss.xml> | TimeoutError:  |
| <https://opentelemetry.io/index.xml> | ClientConnectorError: Cannot connect to host opentelemetry.io:443 ssl:default [None] |
| <https://paddy3118.blogspot.com/feeds/posts/default?alt=rss> | ClientConnectorError: Cannot connect to host paddy3118.blogspot.com:443 ssl:default [None] |
| <https://pervocracy.blogspot.com/feeds/posts/default?alt=rss> | ClientConnectorError: Cannot connect to host pervocracy.blogspot.com:443 ssl:default [None] |
| <https://rite.io/feed/> | ClientConnectorSSLError: Cannot connect to host rite.io:443 ssl:default [[SSL: TLSV1_UNRECOGNIZED_NAME] tlsv1 unrecognized name (_ssl.c:1000)] |
| <https://rworks.dev/index.xml> | ClientConnectorError: Cannot connect to host rworks.dev:443 ssl:default [None] |
| <https://talyarkoni.org/blog/feed/> | TimeoutError:  |
| <https://thore.io/index.xml> | ServerDisconnectedError: Server disconnected |
| <https://v2.tauri.app/blog/rss.xml> | ClientOSError: [Errno 32] Broken pipe |
| <https://voidflower.dev/rss.xml> | ClientConnectorSSLError: Cannot connect to host voidflower.dev:443 ssl:default [[SSL: TLSV1_UNRECOGNIZED_NAME] tlsv1 unrecognized name (_ssl.c:1000)] |
| <https://wasmcloud.com/blog/rss.xml> | ClientConnectorError: Cannot connect to host wasmcloud.com:443 ssl:default [None] |
| <https://weblog.snats.xyz/feed.xml> | ClientConnectorError: Cannot connect to host weblog.snats.xyz:443 ssl:default [None] |
| <https://wonderfall.dev/index.xml> | ClientConnectorError: Cannot connect to host wonderfall.dev:443 ssl:default [Network is unreachable] |
| <https://wonger.dev/rss> | ClientOSError: [Errno 32] Broken pipe |
| <https://www.allendowney.com/blog/feed/> | TimeoutError:  |
| <https://www.blog.philodev.one/index.xml> | ClientConnectorSSLError: Cannot connect to host www.blog.philodev.one:443 ssl:default [[SSL: TLSV1_UNRECOGNIZED_NAME] tlsv1 unrecognized name (_ssl.c:1000)] |
| <https://www.devkitsune.net/blog/wordpress/feed/> | ClientConnectorError: Cannot connect to host www.devkitsune.net:443 ssl:default [None] |
| <https://www.dolthub.com/blog/rss-all.xml> | ClientPayloadError: Response payload is not completed: <ContentLengthError: 400, message='Not enough data to satisfy content length header (received 6631272 of 35046018 bytes).'> |
| <https://www.dvg.blog/feed> | ClientConnectorSSLError: Cannot connect to host www.dvg.blog:443 ssl:default [[SSL: SSLV3_ALERT_HANDSHAKE_FAILURE] sslv3 alert handshake failure (_ssl.c:1000)] |
| <https://www.reachsuite.io/blog-feed.xml> | ClientConnectorCertificateError: Cannot connect to host www.reachsuite.io:443 ssl:True [SSLCertVerificationError: (1, '[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: certificate has expired (_ssl.c:1000)')] |
| <https://www.samstack.io/feed> | ClientConnectorSSLError: Cannot connect to host www.samstack.io:443 ssl:default [[SSL: TLSV1_UNRECOGNIZED_NAME] tlsv1 unrecognized name (_ssl.c:1000)] |
| <https://yetto.app/blog/rss.xml> | ClientConnectorSSLError: Cannot connect to host yetto.app:443 ssl:default [[SSL: TLSV1_UNRECOGNIZED_NAME] tlsv1 unrecognized name (_ssl.c:1000)] |
| <https://blog.kotlin-academy.com/feed> | TimeoutError:  |
| <https://data.xda-developers.com/portal-feed> | ClientConnectorSSLError: Cannot connect to host data.xda-developers.com:443 ssl:default [[SSL: TLSV1_UNRECOGNIZED_NAME] tlsv1 unrecognized name (_ssl.c:1000)] |
| <https://hackaday.com/blog/feed/> | ClientConnectorError: Cannot connect to host hackaday.com:443 ssl:default [None] |
| <https://instagram-engineering.com/feed/> | TimeoutError:  |
| <https://instagram-engineering.com/feed/tagged/android> | TimeoutError:  |
| <https://www.environmentblog.net/category/green-business/feed/> | ClientConnectorSSLError: Cannot connect to host www.environmentblog.net:443 ssl:default [[SSL: TLSV1_UNRECOGNIZED_NAME] tlsv1 unrecognized name (_ssl.c:1000)] |
| <https://blog.isquaredsoftware.com/index.xml> | TimeoutError:  |
| <https://heterodoxacademy.org/blog-rss.xml> | ServerDisconnectedError: Server disconnected |
| <https://blog.hopmembers.com/rss/> | ClientConnectorSSLError: Cannot connect to host www.blog.hopmembers.com:443 ssl:default [[SSL: TLSV1_UNRECOGNIZED_NAME] tlsv1 unrecognized name (_ssl.c:1000)] |
| <https://www.deborahcourtbooks.com/blog-feed.xml> | ClientConnectorError: Cannot connect to host www.deborahcourtbooks.com:443 ssl:default [Connect call failed ('199.15.163.139', 443)] |
| <https://blog.measure.sh/feed> | ClientConnectorSSLError: Cannot connect to host blog.measure.sh:443 ssl:default [[SSL: SSLV3_ALERT_HANDSHAKE_FAILURE] sslv3 alert handshake failure (_ssl.c:1000)] |
| <https://blog.bastion.computer/feed/?type=rss> | ClientConnectorSSLError: Cannot connect to host blog.bastion.computer:443 ssl:default [[SSL: TLSV1_ALERT_INTERNAL_ERROR] tlsv1 alert internal error (_ssl.c:1000)] |
| <https://blog.kevingoldsmith.com/feed/> | TimeoutError:  |
| <https://blog.ardis.dev/feed.xml> | ClientConnectorCertificateError: Cannot connect to host www.blog.ardis.dev:443 ssl:True [SSLCertVerificationError: (1, "[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: Hostname mismatch, certificate is not valid for 'www.blog.ardis.dev'. (_ssl.c:1000)")] |
| <https://www.codingexplorations.com/blog?format=rss> | ClientConnectorCertificateError: Cannot connect to host www.codingexplorations.com:443 ssl:True [SSLCertVerificationError: (1, '[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: unable to get local issuer certificate (_ssl.c:1000)')] |
| <https://blog.bidisaster.party/feed/?type=rss> | TimeoutError:  |
| <https://www.thedailyconservative.org/blog-feed.xml> | ClientConnectorSSLError: Cannot connect to host www.thedailyconservative.org:443 ssl:default [[SSL: TLSV1_UNRECOGNIZED_NAME] tlsv1 unrecognized name (_ssl.c:1000)] |
| <https://blog.grandimam.com/feed.xml> | ClientConnectorSSLError: Cannot connect to host blog.grandimam.com:443 ssl:default [[SSL: TLSV1_UNRECOGNIZED_NAME] tlsv1 unrecognized name (_ssl.c:1000)] |
| <https://19thnews.org/feed> | ClientResponseError: 400, message='Got more than 8190 bytes when reading: b"default-src \'self\'; font-src \'self\' data: https://static.fundraiseup.com https://fonts.gstatic.com; ...".', url='https://19thnews.org/feed' |
| <https://esstnews.com/feed/> | ClientConnectorSSLError: Cannot connect to host esstnews.com:443 ssl:default [[SSL: TLSV1_ALERT_INTERNAL_ERROR] tlsv1 alert internal error (_ssl.c:1000)] |
| <https://roboticsobserver.com/feed/> | ClientConnectorSSLError: Cannot connect to host roboticsobserver.com:443 ssl:default [[SSL: TLSV1_UNRECOGNIZED_NAME] tlsv1 unrecognized name (_ssl.c:1000)] |
| <https://www.digitimes.com/rss/daily.xml> | UnicodeDecodeError: 'utf-8' codec can't decode byte 0xa1 in position 13940: invalid start byte |
| <https://www.gmrnews.com/feed.xml> | ClientConnectorError: Cannot connect to host www.gmrnews.com:443 ssl:default [Connection reset by peer] |
| <https://www.nasa.gov/feed/> | ClientConnectorError: Cannot connect to host www.nasa.gov:443 ssl:default [None] |
| <https://www.thedissident.news/rss/> | ClientConnectorCertificateError: Cannot connect to host www.thedissident.news:443 ssl:True [SSLCertVerificationError: (1, "[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: Hostname mismatch, certificate is not valid for 'www.thedissident.news'. (_ssl.c:1000)")] |
| <https://ngtnews.com/feed> | TimeoutError:  |
| <https://blockfeed.news/rss> | ClientConnectorSSLError: Cannot connect to host blockfeed.news:443 ssl:default [[SSL: TLSV1_UNRECOGNIZED_NAME] tlsv1 unrecognized name (_ssl.c:1000)] |
| <https://bdesk.news/feed/> | ClientConnectorCertificateError: Cannot connect to host www.bdesk.news:443 ssl:True [SSLCertVerificationError: (1, "[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: Hostname mismatch, certificate is not valid for 'www.bdesk.news'. (_ssl.c:1000)")] |
| <https://themeridianews.com/rss.xml> | TimeoutError:  |
| <https://maxglobalnews.com/feed/> | TimeoutError:  |
| <https://blockainews.com/rss/> | ClientConnectorError: Cannot connect to host blockainews.com:443 ssl:default [Connect call failed ('192.241.152.106', 443)] |
| <https://www.nordiskpost.com/feed/> | TimeoutError:  |
| <https://healthcare-newsdesk.co.uk/feed/> | ClientConnectorSSLError: Cannot connect to host healthcare-newsdesk.co.uk:443 ssl:default [[SSL: TLSV1_UNRECOGNIZED_NAME] tlsv1 unrecognized name (_ssl.c:1000)] |
| <https://weaponsofconflict.com/news/rss.xml> | ClientConnectorCertificateError: Cannot connect to host www.weaponsofconflict.com:443 ssl:True [SSLCertVerificationError: (1, "[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: Hostname mismatch, certificate is not valid for 'www.weaponsofconflict.com'. (_ssl.c:1000)")] |
| <https://magnoliatribune.com/home/feed/> | TimeoutError:  |
| <https://evtol.couriernews.co.uk/feed/> | ClientConnectorSSLError: Cannot connect to host evtol.couriernews.co.uk:443 ssl:default [[SSL: TLSV1_UNRECOGNIZED_NAME] tlsv1 unrecognized name (_ssl.c:1000)] |
