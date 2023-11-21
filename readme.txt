
Yêu cầu dự án

###############################
1. Đưa dữ liệu vào MongoDB

###############################
2. Đọc dữ liệu từ MongoDB với Spark

###############################
3. Chuẩn hóa dữ liệu

Do dữ liệu ở MongoDB được import từ csv nên các trường như CreationDate, ClosedDate sẽ được lưu dưới dạng String chứ không phải Datetime nên bạn sẽ cần chuyển về kiểu dữ liệu DateType(), hoặc có một số giá trị trong trường OwnerUserId có giá trị là "NA", bạn cũng sẽ phải chuyển các giá trị "NA" về null và để kiểu dữ liệu là IntegerType(). Sau khi đọc dữ liệu từ Question thì Dataframe sẽ có Schema như sau:

root
 |-- Id: integer (nullable = true)
 |-- OwnerUserId: integer (nullable = true)
 |-- CreationDate: date (nullable = true)
 |-- ClosedDate: date (nullable = true)
 |-- Score: integer (nullable = true)
 |-- Title: string (nullable = true)
 |-- Body: string (nullable = true)

###############################
4. Yêu cầu 1: Tính số lần xuất hiện của các ngôn ngữ lập trình

Với yêu cầu này, bạn sẽ cần đếm số lần mà các ngôn ngữ lập trình xuất hiện trong nội dung của các câu hỏi. Các ngôn ngữ lập trình cần kiểm tra là:


Java, Python, C++, C#, Go, Ruby, Javascript, PHP, HTML, CSS, SQL
Để hoàn thành yêu cầu này, bạn có thể sử dụng regex để trích xuất các ngôn ngữ lập trình đã xuất hiện trong từng câu hỏi. Sau đó sử dụng các phép Aggregation để tính tổng theo từng ngôn ngữ. Kết quả sẽ như sau:

+-------------------+------+                                                    
|Programing Language| Count|
+-------------------+------+
|                 C#| 32414|
|                C++| 28866|
|                CSS| 33556|
|               HTML| 89646|
|                PHP| 63479|
|                SQL|146094|
|                 Go| 79912|
|               Ruby| 16318|
|             Python| 44817|
|               Java|106659|
+-------------------+------+

###############################
5. Yêu cầu 2 : Tìm các domain được sử dụng nhiều nhất trong các câu hỏi

Trong các câu hỏi thường chúng ta sẽ dẫn link từ các trang web khác vào. Ở yêu cầu này, bạn sẽ cần tìm xem 20 domain nào được người dùng sử dụng nhiều nhất. Chú ý rằng các domain sẽ chỉ gồm tên domain, các bạn sẽ không cần trích xuất những tham số phía sau. Ví dụ về một domain: www.google.com, www.facebook.com,...

Để hoàn thành được yêu cầu này, bạn có thể sử dụng regex để trích xuất các url, sau đó áp dung một số biện pháp xử lý chuỗi để lấy ra được tên của domain, cuối cùng là dùng Aggregation để gộp nhóm lại. Kết quả sẽ như sau:

+--------------------+-----+                                                    
|              Domain|Count|
+--------------------+-----+
|   www.cs.bham.ac.uk|    4|
|groups.csail.mit.edu|    7|
|     fiddlertool.com|    1|
|   www.dynagraph.org|    1|
| images.mydomain.com|    1|
|  img7.imageshack.us|    3|
+--------------------+-----+

###############################
6. Yêu cầu 3 : Tính tổng điểm của User theo từng ngày

Bạn cần biết được xem đến ngày nào đó thì User đạt được bao nhiêu điểm. Ví dụ với dữ liệu như sau:

+-----------+------------+-----+
|OwnerUserId|CreationDate|Score|
+-----------+------------+-----+
|         26|  2008-08-01|   26|
|         26|  2008-08-01|  144|
|         83|  2008-08-01|   21|
|    	  83|  2008-08-02|   53|
|         26|  2008-08-02|   29|
+-----------+------------+-----+
Thì bạn sẽ có được kết quả:

+-----------+------------+----------+
|OwnerUserId|CreationDate|TotalScore|
+-----------+------------+----------+
|         26|  2008-08-01|       170|
|         26|  2008-08-02|       199|
|         83|  2008-08-01|        21|
|         83|  2008-08-02|        74|
+-----------+------------+----------+
Để hoàn thành yêu cầu này, bạn sẽ cần sử dụng các thao tác Windowing và các thao tác Aggregation, bạn có thể tham khảo bài Bài 9 : Data Aggregations và Join trên Spark. Kết quả sẽ cần được sắp xếp theo trường OwnerUserId và CreationDate.

###############################
7. Yêu cầu 4: Tính tổng số điểm mà User đạt được trong một khoảng thời gian

Ở yêu cầu này, bạn sẽ cần tính tổng điểm mà User đạt được khi đặt câu hỏi trong một khoảng thời gian. Ví dụ như bạn muốn tính xem từ ngày 01-01-2008 đến 01-01-2009 thì các user đạt được bao nhiêu điểm từ việc đặt câu hỏi. Các khoảng thời gian này sẽ được khai báo trực tiếp trong code, ví dụ như sau:


START = '01-01-2008'
END = '01-01-2009'

Để hoàn thành yêu cầu này, bạn sẽ cần sử dụng filter() để lọc ra các dữ liệu thỏa mãn từ khung dữ liệu, sau đó có thể làm theo yêu cầu 4. Kết quả sẽ cần được sắp xếp theo trường OwnerUserId, ví dụ:

+-----------+----------+
|OwnerUserId|TotalScore|
+-----------+----------+
|       1580|         5|
|       4101|        11|
|      18051|         2|
|      18866|         6|
|    2376109|         5|
+-----------+----------+

###############################
8. Yêu cầu 5: Tìm các câu hỏi có nhiều câu trả lời

Một câu hỏi tốt sẽ được tính số lượng câu trả lời của câu hỏi đó, nếu như câu hỏi có nhiều hơn 5 câu trả lời thì sẽ được tính là tốt. Bạn sẽ cần tìm xem có bao nhiêu câu hỏi đang được tính là tốt,  

Để hoàn thành yêu cầu này, bạn sẽ cần sử dụng các thao tác Join để gộp dữ liệu từ Answers với Collections, sau đó dụng các thao tác Aggregation để gộp nhóm, tính xem mỗi câu hỏi đã có bao nhiêu câu trả lời. Cuối cùng là dùng hàm filter() để lọc ra các câu hỏi có nhiều hơn 5 câu trả lời. 

Lưu ý: Do thao tác có thể tốn rất nhiều thời gian, nên bạn hãy sử dụng cơ chế Bucket Join để phân vùng cho các dữ liệu trước. Bạn có thể tham khảo Bài 9 : Data Aggregations và Join để hiểu rõ hơn về cơ chế này.

Kết quả sẽ cần được sắp xếp theo ID của các câu hỏi

###############################
9. (Nâng cao) Yêu cầu 6: Tìm các Active User

Một User được tính là Active sẽ cần thỏa mãn một trong các yêu cầu sau:

Có nhiều hơn 50 câu trả lời hoặc tổng số điểm đạt được khi trả lời lớn hơn 500.
Có nhiều hơn 5 câu trả lời ngay trong ngày câu hỏi được tạo.
Bạn hãy lọc các User thỏa mãn điều kiện trên.


