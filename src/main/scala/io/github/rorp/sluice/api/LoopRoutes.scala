package io.github.rorp.sluice.api

import akka.http.scaladsl.server.{MalformedFormFieldRejection, Route}
import akka.http.scaladsl.unmarshalling.Unmarshaller
import fr.acinq.bitcoin.scalacompat.Crypto.PublicKey
import fr.acinq.bitcoin.scalacompat.{ByteVector32, Satoshi}
import akka.http.scaladsl.model.StatusCodes
import fr.acinq.eclair.api.directives.{EclairDirectives, ErrorResponse}
import fr.acinq.eclair.ShortChannelId
import fr.acinq.eclair.api.serde.FormParamExtractors._
import io.github.rorp.sluice.SwapManager
import io.github.rorp.sluice.SwapManager.{LoopInRequest, LoopOutRequest}
import io.grpc.StatusRuntimeException

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object LoopRoutes {

  def apply(directives: EclairDirectives, manager: SwapManager): Route = {
    import directives._
    import fr.acinq.eclair.api.serde.JsonSupport.{formats, marshaller, serialization}

    /**
     * eclair's API hides the message of failed calls: we return it for invalid requests and swap server errors, so
     * that users know why their swap was refused (e.g. a fee above their limit).
     */
    def completeOrError[T <: AnyRef](f: => Future[T]): Route = onComplete(Future.unit.flatMap(_ => f)(ExecutionContext.parasitic)) {
      case Success(result) => complete(result)
      case Failure(e: IllegalArgumentException) => complete(StatusCodes.BadRequest, ErrorResponse(e.getMessage))
      case Failure(e: StatusRuntimeException) => complete(StatusCodes.BadGateway, ErrorResponse(s"swap server error: ${e.getStatus.getCode}: ${e.getStatus.getDescription}"))
      case Failure(e) => failWith(e)
    }

    val scidOrAliasUnmarshaller: Unmarshaller[String, ShortChannelId] = Unmarshaller.strict(s => SwapManager.parseShortChannelId(s).get)

    val loopOutTerms: Route = postRequest("loopoutterms") { implicit t =>
      completeOrError(manager.loopOutTerms())
    }

    val loopOutQuote: Route = postRequest("loopoutquote") { implicit t =>
      formFields("amountSat".as[Satoshi], "confTarget".as[Int].?, "fast".as[Boolean].?) { (amount, confTarget_opt, fast_opt) =>
        completeOrError(manager.loopOutQuote(amount, confTarget_opt, fast_opt.getOrElse(false)))
      }
    }

    val loopOut: Route = postRequest("loopout") { implicit t =>
      formFields("amountSat".as[Satoshi], "confTarget".as[Int].?, "fast".as[Boolean].?, "address".?,
        "maxSwapFeeSat".as[Satoshi].?, "maxPrepayAmountSat".as[Satoshi].?, "maxMinerFeeSat".as[Satoshi].?,
        "maxSwapRoutingFeeSat".as[Satoshi].?, "maxPrepayRoutingFeeSat".as[Satoshi].?, "htlcConfirmations".as[Int].?, "firstHop".as[ShortChannelId](scidOrAliasUnmarshaller).?, "idempotencyKey".?) {
        (amount, confTarget_opt, fast_opt, address_opt, maxSwapFee_opt, maxPrepayAmount_opt, maxMinerFee_opt, maxSwapRoutingFee_opt, maxPrepayRoutingFee_opt, htlcConfirmations_opt, firstHop_opt, idempotencyKey_opt) =>
          completeOrError(manager.loopOut(LoopOutRequest(amount, confTarget_opt, fast_opt.getOrElse(false), address_opt, maxSwapFee_opt, maxPrepayAmount_opt, maxMinerFee_opt, maxSwapRoutingFee_opt, maxPrepayRoutingFee_opt, htlcConfirmations_opt, firstHop_opt, idempotencyKey_opt)))
      }
    }

    val listSwaps: Route = postRequest("looplistswaps") { implicit t =>
      complete(manager.listSwaps())
    }

    val swapInfo: Route = postRequest("loopswapinfo") { implicit t =>
      formFields("swapHash".as[ByteVector32]) { swapHash =>
        manager.getSwap(swapHash) match {
          case Some(info) => complete(info)
          case None => reject(MalformedFormFieldRejection("swapHash", s"unknown swap $swapHash"))
        }
      }
    }

    val loopInTerms: Route = postRequest("loopinterms") { implicit t =>
      completeOrError(manager.loopInTerms())
    }

    val loopInQuote: Route = postRequest("loopinquote") { implicit t =>
      formFields("amountSat".as[Satoshi], "lastHop".as[PublicKey].?, "confTarget".as[Int].?, "routeHints".as[Boolean].?) { (amount, lastHop_opt, confTarget_opt, routeHints_opt) =>
        completeOrError(manager.loopInQuote(amount, lastHop_opt, confTarget_opt, routeHints_opt.getOrElse(false)))
      }
    }

    val loopIn: Route = postRequest("loopin") { implicit t =>
      formFields("amountSat".as[Satoshi], "confTarget".as[Int].?, "lastHop".as[PublicKey].?, "address".?, "maxSwapFeeSat".as[Satoshi].?, "maxMinerFeeSat".as[Satoshi].?, "routeHints".as[Boolean].?, "idempotencyKey".?) {
        (amount, confTarget_opt, lastHop_opt, address_opt, maxSwapFee_opt, maxMinerFee_opt, routeHints_opt, idempotencyKey_opt) =>
          completeOrError(manager.loopIn(LoopInRequest(amount, confTarget_opt, lastHop_opt, address_opt, maxSwapFee_opt, maxMinerFee_opt, routeHints_opt.getOrElse(false), idempotencyKey_opt)))
      }
    }

    val abandonSwap: Route = postRequest("loopabandonswap") { implicit t =>
      formFields("swapHash".as[ByteVector32]) { swapHash =>
        completeOrError(manager.abandonSwap(swapHash))
      }
    }

    val loopFetchL402: Route = postRequest("loopfetchl402") { implicit t =>
      completeOrError(manager.fetchL402())
    }

    val loopListL402: Route = postRequest("looplistl402") { implicit t =>
      complete(manager.listL402Tokens())
    }

    loopOutTerms ~ loopOutQuote ~ loopOut ~ listSwaps ~ swapInfo ~ abandonSwap ~ loopInTerms ~ loopInQuote ~ loopIn ~ loopFetchL402 ~ loopListL402
  }

}
