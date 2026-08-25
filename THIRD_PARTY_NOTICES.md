# Third-party notices

## intfloat/multilingual-e5-small

agentd uses the weights from
[`intfloat/multilingual-e5-small`](https://huggingface.co/intfloat/multilingual-e5-small)
at revision `614241f622f53c4eeff9890bdc4f31cfecc418b3`. It downloads and redistributes
the portable INT8 ONNX conversion and matching tokenizer assets from
[`Xenova/multilingual-e5-small`](https://huggingface.co/Xenova/multilingual-e5-small)
at revision `761b726dd34fb83930e26aab4e9ac3899aa1fa78`.

The model card at that pinned revision declares `license: mit`, but the model
repository does not contain a standalone license file. The upstream
[E5 documentation](https://github.com/microsoft/unilm/tree/master/e5) points to
the root UniLM license, which is the MIT license with a Microsoft Corporation
copyright notice. A copy of that upstream license is distributed in
[`licenses/multilingual-e5-small.LICENSE`](licenses/multilingual-e5-small.LICENSE)
and beside the model assets in the container image. This notice records the
source of the license text; it does not change agentd's Apache-2.0 license.

The bundled asset set is limited to:

- `config.json`
- `onnx/model_int8.onnx`
- `special_tokens_map.json`
- `tokenizer.json`
- `tokenizer_config.json`

## BAAI/bge-reranker-v2-m3

agentd uses the Apache-2.0-licensed
[`BAAI/bge-reranker-v2-m3`](https://huggingface.co/BAAI/bge-reranker-v2-m3)
model. It downloads and redistributes the INT8 ONNX conversion and matching
tokenizer assets from
[`onnx-community/bge-reranker-v2-m3-ONNX`](https://huggingface.co/onnx-community/bge-reranker-v2-m3-ONNX)
at revision `6f5ff65298512715a1e669753bc754d2bc8f367b`. The standard Apache License 2.0
text in the repository's [`LICENSE`](LICENSE) is copied beside the model assets
in the container image.

The bundled asset set is limited to:

- `config.json`
- `onnx/model_int8.onnx`
- `special_tokens_map.json`
- `tokenizer.json`
- `tokenizer_config.json`
