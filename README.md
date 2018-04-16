# Hadoop-labs

모든 분석 작업은 MapReduce 프로그램으로 작성

로그 메시지 설명 사이트 - http://jmcauley.ucsd.edu/data/amazon/

1. /shared-data/reviews_Books_5.json 의 전체 상품 평균 "overall" 점수 분석
2. /shared-data/reviews_Books_5.json 에서 가장 많은 리뷰를 남긴 사용자 아이디("reviewerID") 및 리뷰 횟수는 ?
3. /shared-data/reviews_Books_5.json 에서 helpful 필드는 [a,b] 형식을 가지며, b명의 사용자가
   해당 리뷰가 도움이 되는지 투표했다는 의미이며, 이중 a 명의 사용자가 도움이 된다는 의견을
   남겼다는 의미이다. b값이 10보다 큰 사용자 중에서 도움이 된다고 하는 사람의 비율(a/b)이 가장 높은
   아이템 (asin) 의 아이디는?
4. /shared-data/reviews_Books_5.json 에서 각 reviewer 별로 helpful 필드 값을 모았을때 ([a,b] 를 a 값과 b 값으로 각각 
   더함), 가장 높은 a 값을 가지는 사용자의 (reviewerID) 아아디는?
5. /shared-data/reviews_Books_5.json 에서 각 reviewerText에서 가장 긴 리뷰를 남긴 reviewerName 값은?