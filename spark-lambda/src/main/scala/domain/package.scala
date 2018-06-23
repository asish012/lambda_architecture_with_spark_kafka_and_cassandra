package object domain {
    case class Activity (timestamp_hour : Long,
                         referrer : String,
                         action : String,
                         prevPage : String,
                         page : String,
                         visitor : String,
                         product : String,
                         inputProps : Map[String, String] = Map()
                        )

    case class ActivityByProduct(timestamp_hour : Long,
                                 product : String,
                                 purchase_count : Long,
                                 add_to_cart_count : Long,
                                 page_view_count : Long)
}
