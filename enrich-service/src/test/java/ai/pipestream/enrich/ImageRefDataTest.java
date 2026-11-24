package ai.pipestream.enrich;

import static org.assertj.core.api.Assertions.assertThat;

import ai.pipestream.document.v1.BaseTextItem;
import ai.pipestream.document.v1.CodeItem;
import ai.pipestream.document.v1.DocItemLabel;
import ai.pipestream.document.v1.Document;
import ai.pipestream.document.v1.ImageRef;
import ai.pipestream.document.v1.PictureItem;
import ai.pipestream.enrich.engine.EndpointPolicy;
import ai.pipestream.enrich.engine.ItemSelector;
import ai.pipestream.enrich.engine.ItemSelector.Selection;
import ai.pipestream.enrich.v1.AnnotationInput;
import ai.pipestream.enrich.v1.EnrichOptions;
import ai.pipestream.enrich.v1.ItemImage;
import ai.pipestream.enrich.vlm.VlmClient.VlmImage;
import com.google.protobuf.ByteString;
import java.util.Base64;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * An image a producer holds travels as the bytes in {@code ImageRef.data},
 * not as a base64 {@code data:} URI in {@code ImageRef.uri}. Selection reads
 * those bytes: a picture or code item that carries them is sent to the VLM
 * with them, exactly as one that spells the same image as a data URI is.
 */
class ImageRefDataTest {

  private static final ByteString JPEG =
      ByteString.copyFrom(new byte[] {(byte) 0xFF, (byte) 0xD8, (byte) 0xFF, 1, 2, 3});

  private static final EnrichOptions DESCRIBE = EnrichOptions.newBuilder()
      .setDoPictureDescription(true)
      .setPictureDescriptionAreaThreshold(-1)
      .build();

  private static Document pictureWith(ImageRef.Builder image) {
    return Document.newBuilder()
        .setName("held-bytes")
        .addPictures(PictureItem.newBuilder()
            .setSelfRef("#/pictures/0")
            .setLabel(DocItemLabel.DOC_ITEM_LABEL_PICTURE)
            .setImage(image))
        .build();
  }

  private static Document codeWith(ImageRef.Builder image) {
    return Document.newBuilder()
        .setName("held-bytes")
        .addTexts(BaseTextItem.newBuilder()
            .setCode(CodeItem.newBuilder()
                .setSelfRef("#/texts/0")
                .setLabel(DocItemLabel.DOC_ITEM_LABEL_CODE)
                .setText("print( 1 )")
                .setImage(image)))
        .build();
  }

  @Test
  void pictureHoldingBytes_isSelectedWithThoseBytes() {
    Selection selection = ItemSelector.select(
        pictureWith(ImageRef.newBuilder().setMimetype("image/jpeg").setData(JPEG)),
        Map.of(), DESCRIBE);

    assertThat(selection.skips()).isEmpty();
    assertThat(selection.work()).hasSize(1);
    assertThat(selection.work().get(0).image())
        .isInstanceOfSatisfying(VlmImage.Bytes.class, held -> {
          // Referenced, not copied or encoded: base64 waits for the call.
          assertThat(held.data()).isSameAs(JPEG);
          assertThat(held.mimetype()).isEqualTo("image/jpeg");
        });
  }

  @Test
  void heldBytes_winOverAReferenceToWhereTheImageCameFrom() {
    // Bytes plus a reference the server would never fetch: the picture is
    // not unfetchable, its image is right there.
    Selection selection = ItemSelector.select(
        pictureWith(ImageRef.newBuilder()
            .setMimetype("image/jpeg")
            .setData(JPEG)
            .setUri("https://example.test/figure.jpg")),
        Map.of(), DESCRIBE);

    assertThat(selection.skips()).isEmpty();
    assertThat(selection.work()).hasSize(1);
    assertThat(selection.work().get(0).image())
        .isInstanceOfSatisfying(VlmImage.Bytes.class,
            held -> assertThat(held.data()).isSameAs(JPEG));
  }

  @Test
  void heldBytes_winOverADataUriOfTheSameField() {
    // A producer in the middle of moving from the URI to the bytes may fill
    // both. The bytes are the image; the URI is not decoded a second time.
    String uri = "data:image/png;base64,"
        + Base64.getEncoder().encodeToString(new byte[] {9, 9, 9});
    Selection selection = ItemSelector.select(
        pictureWith(ImageRef.newBuilder().setMimetype("image/jpeg").setData(JPEG).setUri(uri)),
        Map.of(), DESCRIBE);

    assertThat(selection.work()).hasSize(1);
    assertThat(selection.work().get(0).image())
        .isInstanceOfSatisfying(VlmImage.Bytes.class,
            held -> assertThat(held.data()).isSameAs(JPEG));
  }

  @Test
  void anItemImageCrop_stillWinsOverHeldBytes() {
    ByteString crop = ByteString.copyFrom(new byte[] {7, 7, 7, 7});
    Selection selection = ItemSelector.select(
        pictureWith(ImageRef.newBuilder().setMimetype("image/jpeg").setData(JPEG)),
        Map.of("#/pictures/0", ItemImage.newBuilder()
            .setSelfRef("#/pictures/0").setMimetype("image/png").setData(crop).build()),
        DESCRIBE);

    assertThat(selection.work()).hasSize(1);
    assertThat(selection.work().get(0).image())
        .isInstanceOfSatisfying(VlmImage.Bytes.class, sent -> {
          assertThat(sent.data()).isSameAs(crop);
          assertThat(sent.mimetype()).isEqualTo("image/png");
        });
  }

  @Test
  void codeItemHoldingBytes_isSentAsAnImage() {
    Selection selection = ItemSelector.select(
        codeWith(ImageRef.newBuilder().setMimetype("image/jpeg").setData(JPEG)),
        Map.of(), EnrichOptions.newBuilder().setDoCodeEnrichment(true).build());

    assertThat(selection.skips()).isEmpty();
    assertThat(selection.work()).hasSize(1);
    assertThat(selection.work().get(0).image())
        .as("a code item with its image in ImageRef.data is not sent as text only")
        .isInstanceOfSatisfying(VlmImage.Bytes.class,
            held -> assertThat(held.data()).isSameAs(JPEG));
  }

  @Test
  void heldBytes_reachTheVlmAsTheSameImageADataUriWouldBe() throws Exception {
    try (FakeVlmServer vlm = new FakeVlmServer();
        InProcessEnrich enrich = InProcessEnrich.start(EndpointPolicy.defaultOnly(vlm.url()))) {
      vlm.responder = body -> "a figure";
      InProcessEnrich.Collected result = enrich.run(DESCRIBE.toBuilder()
          .setDocument(pictureWith(ImageRef.newBuilder().setMimetype("image/jpeg").setData(JPEG)))
          .build());

      assertThat(result.error()).isNull();
      assertThat(result.skips()).isEmpty();
      assertThat(result.annotations()).hasSize(1);
      assertThat(result.annotations().get(0).getInput())
          .isEqualTo(AnnotationInput.ANNOTATION_INPUT_IMAGE);
      assertThat(vlm.recorded()).hasSize(1);
      assertThat(vlm.recorded().get(0).body())
          .contains("data:image/jpeg;base64," + Base64.getEncoder().encodeToString(JPEG.toByteArray()));
    }
  }
}
