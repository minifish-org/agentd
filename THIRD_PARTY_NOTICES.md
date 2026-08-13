# Third-party notices

## intfloat/multilingual-e5-small

agentd downloads and redistributes selected ONNX and tokenizer assets from
[`intfloat/multilingual-e5-small`](https://huggingface.co/intfloat/multilingual-e5-small)
at revision `614241f622f53c4eeff9890bdc4f31cfecc418b3`.

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
- `onnx/model.onnx`
- `special_tokens_map.json`
- `tokenizer.json`
- `tokenizer_config.json`
