class ContinuityDev < Formula
  desc "Persistent memory for AI coding agents (rolling dev build of main)"
  homepage "https://github.com/lazypower/continuity"
  version "0.0.0-dev"
  license "MIT"

  # Tracks the tip of main via the rolling "dev" prerelease and installs the
  # same "continuity" binary as the stable formula, so the dev and stable
  # channels are mutually exclusive.
  #
  # This file is a template. The published formula is regenerated on every push
  # to main by .github/workflows/dev-release.yml (version + sha256 filled in)
  # and committed to lazypower/homebrew-tap.
  conflicts_with "continuity", because: "both install the continuity binary"

  on_macos do
    on_arm do
      url "https://github.com/lazypower/continuity/releases/download/dev/continuity-darwin-arm64"
      sha256 "PLACEHOLDER"

      def install
        bin.install "continuity-darwin-arm64" => "continuity"
      end
    end

    on_intel do
      url "https://github.com/lazypower/continuity/releases/download/dev/continuity-darwin-amd64"
      sha256 "PLACEHOLDER"

      def install
        bin.install "continuity-darwin-amd64" => "continuity"
      end
    end
  end

  on_linux do
    on_arm do
      url "https://github.com/lazypower/continuity/releases/download/dev/continuity-linux-arm64"
      sha256 "PLACEHOLDER"

      def install
        bin.install "continuity-linux-arm64" => "continuity"
      end
    end

    on_intel do
      url "https://github.com/lazypower/continuity/releases/download/dev/continuity-linux-amd64"
      sha256 "PLACEHOLDER"

      def install
        bin.install "continuity-linux-amd64" => "continuity"
      end
    end
  end

  test do
    assert_match "continuity", shell_output("#{bin}/continuity version")
  end
end
