from cmp.llm_parser import parse_with_llm


def parse_query(text: str):

    try:
        llm_response = parse_with_llm(text)

        return {
            "llm_raw": llm_response
        }


    except Exception as e:

        return {

            "error": str(e),

            "raw_text": text

        }