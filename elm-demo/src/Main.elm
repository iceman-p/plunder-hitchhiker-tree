port module Main exposing (main)

import Json.Decode as Json
import Json.Encode
import String
import Tuple
import Browser
import Dict exposing (..)
import Html.Attributes exposing (..)
import Html.Events exposing (..)
import Html exposing (..)

type alias Name = String

type UserCmd
    = UC_Search (List String)
    | UC_Info Int

type Response
    = SearchResponseBadTag (List String)
    | SearchResponseOK (List (Int, String))
--    | SearchResponseInfo Int (List String) String


encodeUserCmd : UserCmd -> Json.Value
encodeUserCmd cmd = case cmd of
    UC_Search tags -> Json.Encode.object [
        ("tag", Json.Encode.string "Search"),
        ("contents", Json.Encode.list Json.Encode.string tags)
        ]
    UC_Info imgId -> Json.Encode.object [
        ("tag", Json.Encode.string "Info"),
        ("contents", Json.Encode.int imgId)
        ]

decodeTagged : String -> Json.Decoder a -> Json.Decoder a
decodeTagged tag more =
    let
        check got =
            if got == tag then
                more
            else
                Json.fail ("Expected _.tag = " ++ tag)
    in
        Json.field "tag" Json.string
     |> Json.andThen check

decodeTaggedContents : String -> Json.Decoder a -> Json.Decoder a
decodeTaggedContents tag contents =
    decodeTagged tag (Json.field "contents" contents)

decodeResponse : Json.Encode.Value -> Result Json.Error Response
decodeResponse = Json.decodeValue <| Json.oneOf
    [ decodeTaggedContents "BadTag"
        (Json.map SearchResponseBadTag
          (Json.list Json.string)),
      decodeTaggedContents "OK"
        (Json.map SearchResponseOK
          (Json.list
            (Json.map2 Tuple.pair
                       (Json.index 0 Json.int)
                       (Json.index 1 Json.string))))
    ]

sendCmd : UserCmd -> Cmd msg
sendCmd uc = sendMessage (encodeUserCmd uc)


main : Program Int Model Msg
main =
  Browser.element
    { init = init
    , subscriptions = (\_ -> messageReceiver Recv)
    , update = update
    , view = view
    }

type alias PendingQuery = List String

-- What state can the demo be in?

type alias ImgResults = List (Int, String)

type alias BadTagResult = List String

type QueryState
    -- Empty box, show nothing
    = QueryEmpty
    -- Show the last known good result, waiting query and maybe pending query.
    | QueryWaiting (Maybe ImgResults) (List String) (Maybe (List String))
    -- Show the last known good result and the current bad result.
    | QueryComplete (Maybe ImgResults) (Maybe BadTagResult)

prevImgResults : QueryState -> Maybe ImgResults
prevImgResults qs = case qs of
    QueryEmpty -> Nothing
    QueryWaiting i _ _ -> i
    QueryComplete i _ -> i

type alias Model =
    { rawQuery : String
    , queryState : QueryState
    }

init : Int -> (Model, Cmd Msg)
init i = ({ rawQuery = "", queryState = QueryEmpty }, Cmd.none)

-- Update

type Msg
    = ChangeQuery String
    | Recv Json.Encode.Value

noCmd : a -> (a, Cmd b)
noCmd a = (a, Cmd.none)



port sendMessage : Json.Encode.Value -> Cmd msg
port messageReceiver : (Json.Encode.Value -> msg) -> Sub msg

sendSearch : Model -> String -> (Model, Cmd Msg)
sendSearch model query =
    let tokenized = tokenizeTags query
    in case model.queryState of
      QueryEmpty ->
        ({ model
           | rawQuery = query,
             queryState = QueryWaiting Nothing tokenized Nothing },
         sendCmd (UC_Search tokenized))
      QueryWaiting results cur _ ->
        ({ model
           | rawQuery = query,
             queryState = QueryWaiting results cur (Just tokenized) },
         Cmd.none)
      QueryComplete results _ ->
        ({ model
           | rawQuery = query,
             queryState = QueryWaiting results tokenized Nothing },
         sendCmd (UC_Search tokenized))

-- Called when we received an answer to a previous query and may need to kick
-- off the next one.
getNextQuery : Model -> Model -> (Model, Cmd Msg)
getNextQuery oldModel newModel = case oldModel.queryState of
  QueryWaiting _ cur (Just next) ->
      if cur /= next then (newModel, sendCmd (UC_Search next))
                     else (newModel, Cmd.none)
  _ -> (newModel, Cmd.none)

update : Msg -> Model -> (Model, Cmd Msg)
update msg model =
  case msg of
    ChangeQuery newQuery -> sendSearch model newQuery

    Recv jsonMsg -> case (decodeResponse jsonMsg) of
        Err e ->
            let foo = Debug.log "PORT RECV ERR2" e
            in noCmd model
        Ok (SearchResponseBadTag badTags) ->
            getNextQuery model { model
              | queryState =
                  QueryComplete (prevImgResults model.queryState) (Just badTags)
            }
        Ok (SearchResponseOK imgResults) ->
            getNextQuery model { model
              | queryState = QueryComplete (Just imgResults) Nothing }

-- VIEW

tokenizeTags : String -> List String
tokenizeTags str =
    List.filter (\a -> (not (String.isEmpty a)))
                (List.map String.trim (String.split "," str))

tagView : String -> Html Msg
tagView t = div [] [ text t ]

imgResult : (Int, String) -> Html Msg
imgResult (id, thumbnail) =
    div [ style "flex-basis" "350px"
        , style "max-width" "250px"
        , style "max-height" "250px"
        , style "overflow" "hidden"
        ]
        [img [ src thumbnail
             , style "object-fit" "cover"
             , style "max-width" "100%"
             , style "height" "auto"
             , style "justify-content" "center"
             , style "vertical-align" "middle"
             ] []]

imgLatestResults : ImgResults -> Html Msg
imgLatestResults imgResults = div [] [
    text ("Found " ++ String.fromInt (List.length imgResults) ++ ". Showing first 25."),
    div [ style "display" "flex"
        , style "flex-wrap" "wrap"
        , style "justify-content" "center"
        , style "gap" "5px"
        ]
        (List.map imgResult (List.take 25 imgResults))
    ]

imgDiv : QueryState -> Html Msg
imgDiv t = case t of
    QueryEmpty -> div [] [ text "Type to search" ]
    QueryWaiting mybImgResults _ _ -> case mybImgResults of
        Nothing -> div [] [ text "Searching..." ]
        Just imgResults -> imgLatestResults imgResults
    QueryComplete mybImgResults _ -> case mybImgResults of
        Nothing -> div [] [ text "No results." ]
        Just imgResults -> imgLatestResults imgResults


view : Model -> Html Msg
view model =
  div []
    [ h3 [] [text "Tag Search"]
    , input [ placeholder "Search", value model.rawQuery, onInput ChangeQuery ] []
    , div [] (List.map tagView (tokenizeTags model.rawQuery))
    , imgDiv model.queryState
    ]



-- What's the interface between the server and the client?
--
-- Client needs to be able to:
--
-- V0:
--
-- - Ask for imgids that match a query, or non-existent tags. (json)
--     : List String -> Either (List String) (List Int)
--
--
-- V1:
--
-- - Ask for the thumb (http)
--     : Int -> Bytestring
--
-- - Ask for the medium (http)
--     : Int -> Bytestring
