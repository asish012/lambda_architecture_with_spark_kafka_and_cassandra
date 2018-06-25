import domain.ActivityByProduct
import org.apache.spark.streaming.State

package object functions {

    val mapActivityStateFunction = (k : (Long, String), v : Option[ActivityByProduct], state : State[(Long, Long, Long)]) => {
        var (purchase_count, add_to_cart_count, page_view_count) = state.getOption().getOrElse((0L, 0L, 0L))
        print(s">>>>> Existing state of ${k._2}: $purchase_count, $add_to_cart_count, $page_view_count \n")

        val newVal = v match {
            case Some(a : ActivityByProduct) => (a.purchase_count, a.add_to_cart_count, a.page_view_count)
            case _ => (0L, 0L, 0L)
        }

        // update the state
        purchase_count += newVal._1
        add_to_cart_count += newVal._2
        page_view_count += newVal._3

        state.update((purchase_count, add_to_cart_count, page_view_count))
        var (purchase_count2, add_to_cart_count2, page_view_count2) = state.getOption().getOrElse((0L, 0L, 0L))
        print(s">>>>> Updated state of ${k._2}: $purchase_count2, $add_to_cart_count2, $page_view_count2 \n")

        // Mapped Type
//        val underExposed = {
//            if (purchase_count == 0)
//                0
//            else
//                page_view_count / purchase_count
//        }
//        underExposed

        ActivityByProduct(k._1, k._2, purchase_count, add_to_cart_count, page_view_count)
    }


}
